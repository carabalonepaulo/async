package async

import "core:testing"
import "core:time"
import "coro"
import "storage"

@(private = "file")
Inner_Cancel_Token :: struct {
	waiters: Waiters,
}

Cancel_Token :: distinct u64

@(deprecated = "'Cancellation_Token deprecated, use 'Cancel_Token' instead")
Cancellation_Token :: Cancel_Token

create_cancel_token :: proc() -> Cancel_Token {
	res := Resource {
		id = auto_cast Internal_Resource.Cancel_Token,
		drop = proc(self: ^Resource) {
			inner := load_inline(&self.ud, Inner_Cancel_Token)
			deinit_waiters(&inner.waiters)
		},
	}
	inner := load_inline(&res.ud, Inner_Cancel_Token)
	init_waiters(&inner.waiters)

	sched := get_scheduler()
	id := storage.add(&sched.resources, res)
	return transmute(Cancel_Token)(id)
}

destroy_cancel_token :: proc(self: Cancel_Token) {
	sched := get_scheduler()
	res, ok := storage.remove(&sched.resources, u64(self))
	assert(ok, "invalid cancel token")

	inner := load_inline(&res.ud, Inner_Cancel_Token)
	deinit_waiters(&inner.waiters)
}

trigger :: destroy_cancel_token

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
	add_waiter(&inner.waiters, handle, -1)
	yield()
}

cancel_token_branch :: proc(self: Cancel_Token) -> (c: Case) {
	State :: struct {
		cancel: Cancel_Token,
		waiter: Waiter,
	}

	ud := [CASE_INLINE_STORAGE]rawptr{}
	load_inline(&ud, State)^ = State{self, {}}

	return Case {
		ud = ud, //
		is_alive = proc(self: ^Case) -> bool {
			state := load_inline(&self.ud, State)
			sched := get_scheduler()
			_, ok := storage.get_ptr(&sched.resources, transmute(u64)(state.cancel))
			return ok
		},
		try = proc(self: ^Case) -> bool {return false},
		complete = proc(self: ^Case, ok: bool) {},
		subscribe = proc(self: ^Case, handle: Handle, case_idx: int) {
			state := load_inline(&self.ud, State)
			inner := get_inner(state.cancel)
			state.waiter = add_waiter(&inner.waiters, handle, case_idx)
		},
		unsubscribe = proc(self: ^Case, handle: Handle) {
			state := load_inline(&self.ud, State)
			inner := get_inner(state.cancel)
			try_remove_waiter_by_id(&inner.waiters, state.waiter)
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

