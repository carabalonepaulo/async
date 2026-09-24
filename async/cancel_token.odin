package async

import "core:testing"
import "core:time"
import "coro"
import "storage"

@(private = "file")
DEFAULT_WAITERS_CAP :: 2

@(private = "file")
Waiter :: struct {
	handle:   Handle,
	case_idx: int,
}

@(private = "file")
Inner_Cancel_Token :: struct {
	waiters: map[Handle]Waiter,
}

Cancel_Token :: distinct u64

@(deprecated = "'Cancellation_Token deprecated, use 'Cancel_Token' instead")
Cancellation_Token :: Cancel_Token

create_cancel_token :: proc() -> Cancel_Token {
	res := Resource {
		id = auto_cast Internal_Resource.Cancel_Token,
		drop = proc(self: ^Resource) {
			inner := load_inline(&self.ud, Inner_Cancel_Token)
			delete(inner.waiters)
		},
	}
	inner := load_inline(&res.ud, Inner_Cancel_Token)
	inner.waiters = make(map[Handle]Waiter, DEFAULT_WAITERS_CAP)

	sched := get_scheduler()
	id := storage.add(&sched.resources, res)
	return transmute(Cancel_Token)(id)
}

destroy_cancel_token :: proc(self: Cancel_Token) {
	sched := get_scheduler()
	res, ok := storage.remove(&sched.resources, u64(self))
	assert(ok, "invalid cancel token")

	inner := load_inline(&res.ud, Inner_Cancel_Token)
	for _, waiter in inner.waiters {
		if waiter.case_idx == -1 do wake(waiter.handle)
		else do wake_case(waiter.handle, waiter.case_idx, false)
	}
	delete(inner.waiters)
}

trigger :: proc(self: Cancel_Token) {
	destroy_cancel_token(self)
}

is_triggered :: proc(self: Cancel_Token) -> bool {
	sched := get_scheduler()
	_, ok := storage.get_ptr(&sched.resources, u64(self))
	return !ok
}

cancel_after :: proc(self: Cancel_Token, n: time.Duration) {
	timer(n, auto_cast proc(self: Cancel_Token) {trigger(self)}, transmute(rawptr)(self))
}

cancel_token_wait :: proc(self: Cancel_Token) {
	handle := get_handle()
	inner, ok := try_get_inner(self)
	if !ok do return
	inner.waiters[handle] = Waiter{handle, -1}
	yield()
}

cancel_token_branch :: proc(self: Cancel_Token) -> (c: Case) {
	ud := [CASE_INLINE_STORAGE]rawptr{}
	ud[0] = transmute(rawptr)(self)

	return Case {
		ud = ud, //
		is_alive = proc(self: ^Case) -> bool {
			id := transmute(u64)(self.ud[0])
			sched := get_scheduler()
			_, ok := storage.get_ptr(&sched.resources, id)
			return ok
		},
		try = proc(self: ^Case) -> bool {return false},
		complete = proc(self: ^Case, ok: bool) {},
		subscribe = proc(self: ^Case, handle: Handle, case_idx: int) {
			tk := transmute(Cancel_Token)(self.ud[0])
			inner := get_inner(tk)
			inner.waiters[handle] = Waiter{handle, case_idx}
		},
		unsubscribe = proc(self: ^Case, handle: Handle) {
			tk := transmute(Cancel_Token)(self.ud[0])
			inner := get_inner(tk)
			delete_key(&inner.waiters, handle)
		},
	}
}

@(private = "file")
try_get_inner :: proc(self: Cancel_Token) -> (inner: ^Inner_Cancel_Token, ok: bool) {
	sched := get_scheduler()
	res := storage.get_ptr(&sched.resources, u64(self)) or_return
	return load_inline(&res.ud, Inner_Cancel_Token), true
}

@(private = "file")
get_inner :: proc(self: Cancel_Token) -> ^Inner_Cancel_Token {
	inner, ok := try_get_inner(self)
	assert(ok, "invalid cancel token")
	return inner
}

cancel_token_into_rawptr :: #force_inline proc(self: Cancel_Token) -> rawptr {
	return transmute(rawptr)(self)
}

@(test)
test_wait :: proc(t: ^testing.T) {
	init()
	defer deinit()

	cancel := create_cancel_token()
	count := 0

	a := spawn(&count, cancel, proc(count: ^int, cancel: Cancel_Token) {
		wait(cancel)
		count^ += 1
	})

	b := spawn(&count, cancel, proc(count: ^int, cancel: Cancel_Token) {
		wait(cancel)
		count^ += 1
	})

	c := spawn(&count, cancel, t, proc(count: ^int, cancel: Cancel_Token, t: ^testing.T) {
		trigger(cancel)
		reschedule()
		testing.expect(t, count^ == 2)
	})

	block(c)
}

@(test)
test_select :: proc(t: ^testing.T) {
	init()
	defer deinit()

	State :: struct {
		a:      Handle,
		b:      Handle,
		c:      Handle,
		//
		wg:     Wait_Group,
		cancel: Cancel_Token,
		t:      ^testing.T,
	}

	state: State
	state.cancel = create_cancel_token()
	state.wg = create_wait_group()
	add(state.wg, 2)
	defer destroy(state.wg)

	state.a = spawn(&state, proc(state: ^State) {
		idx := select({branch(state.cancel)})
		testing.expect(state.t, idx == 0)

		inner := get_current_internal_state()
		testing.expect(state.t, coro.get_bytes_stored(inner.co) == 0)

		done(state.wg)
	})

	state.b = spawn(&state, proc(state: ^State) {
		idx := select({branch(state.cancel)})
		testing.expect(state.t, idx == 0)

		inner := get_current_internal_state()
		testing.expect(state.t, coro.get_bytes_stored(inner.co) == 0)

		done(state.wg)
	})

	state.c = spawn(&state, proc(state: ^State) {
		trigger(state.cancel)
		wait(state.wg)
	})

	block(state.c)
}

