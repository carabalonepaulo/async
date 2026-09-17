package async

import "core:time"
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
			inner := resource_as_inner(self)
			delete(inner.waiters)
		},
	}
	inner := resource_as_inner(&res)
	inner.waiters = make(map[Handle]Waiter, DEFAULT_WAITERS_CAP)

	sched := get_scheduler()
	id := storage.add(&sched.resources, res)
	return transmute(Cancel_Token)(id)
}

destroy_cancel_token :: proc(self: Cancel_Token) {
	sched := get_scheduler()
	res, ok := storage.remove(&sched.resources, u64(self))
	assert(ok, "invalid cancel token")

	inner := resource_as_inner(&res)
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
	return Case {
		ud = [MAX_USER_DATA]rawptr{transmute(rawptr)(self), nil, nil, nil, nil},
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
	return resource_as_inner(res), true
}

@(private = "file")
get_inner :: proc(self: Cancel_Token) -> ^Inner_Cancel_Token {
	inner, ok := try_get_inner(self)
	assert(ok, "invalid cancel token")
	return inner
}

@(private = "file")
resource_as_inner :: proc(res: ^Resource) -> ^Inner_Cancel_Token {
	#assert(size_of([MAX_USER_DATA]rawptr) >= size_of(Inner_Cancel_Token))
	#assert(align_of([MAX_USER_DATA]rawptr) >= align_of(Inner_Cancel_Token))
	return transmute(^Inner_Cancel_Token)(&res.ud[0])
}

cancel_token_into_rawptr :: #force_inline proc(self: Cancel_Token) -> rawptr {
	return transmute(rawptr)(self)
}

