package async

import "core:container/queue"
import "storage"

@(private = "file")
Waiter :: struct {
	handle:   Handle,
	case_idx: int,
}

@(private = "file")
Inner_Semaphore :: struct {
	n:       int,
	cap:     int,
	waiters: queue.Queue(Waiter),
}

Semaphore :: distinct u64

create_semaphore :: proc(n: int) -> Semaphore {
	assert(n > 0, "semaphore count must be positive")

	inner := new(Inner_Semaphore)
	inner.n = n
	inner.cap = n
	queue.init(&inner.waiters)

	res := Resource {
		id = auto_cast Internal_Resource.Semaphore,
		ud = [MAX_USER_DATA]rawptr{inner, nil, nil, nil, nil},
	}
	sched := get_scheduler()
	id := storage.add(&sched.resources, res)
	return Semaphore(id)
}

semaphore_destroy :: proc(self: Semaphore) {
	sched := get_scheduler()
	res, ok := storage.remove(&sched.resources, u64(self))
	assert(ok, "invalid semaphore")

	inner := (^Inner_Semaphore)(res.ud[0])
	for queue.len(inner.waiters) > 0 {
		waiter := queue.pop_front(&inner.waiters)
		if waiter.case_idx == -1 do send(waiter.handle, false)
		else do wake_case(waiter.handle, waiter.case_idx, false)
	}

	queue.destroy(&inner.waiters)
	free(inner)
}

try_acquire :: proc(self: Semaphore) -> bool {
	inner := get_inner(self)
	if inner.n > 0 {
		inner.n -= 1
		return true
	}
	return false
}

acquire :: proc(self: Semaphore, cancel: Maybe(Cancel_Token) = nil) -> (ok: bool) {
	inner := get_inner(self)

	if inner.n > 0 {
		inner.n -= 1
		return true
	}

	if cancel, cancel_ok := cancel.(Cancel_Token); cancel_ok {
		idx := select({branch(cancel), branch(self, &ok)})
		return idx == 0 ? false : ok
	} else {
		queue.enqueue(&inner.waiters, Waiter{get_handle(), -1})
		return recv(bool)
	}

	return false
}

release :: proc(self: Semaphore) {
	inner := get_inner(self)

	if waiter, ok := queue.pop_front_safe(&inner.waiters); ok {
		if waiter.case_idx == -1 do send(waiter.handle, true)
		else do wake_case(waiter.handle, waiter.case_idx, true)
	} else {
		assert(inner.n < inner.cap)
		inner.n += 1
	}
}

semaphore_branch :: proc(self: Semaphore, out_ok: ^bool) -> Case {
	User_Data :: enum {
		Id,
		Out_Ok,
	}

	get :: #force_inline proc(self: ^Case, ud: User_Data, $A: typeid) -> A {
		return transmute(A)(self.ud[ud])
	}

	return Case {
		ud = [MAX_USER_DATA]rawptr{transmute(rawptr)(self), out_ok, nil, nil, nil},
		is_alive = proc(self: ^Case) -> bool {
			id := get(self, .Id, u64)
			sched := get_scheduler()
			_, ok := storage.get_ptr(&sched.resources, id)
			return ok
		},
		try = proc(self: ^Case) -> (ok: bool) {
			sem := transmute(Semaphore)(self.ud[0])
			if ok = try_acquire(sem); ok {
				out_ok := get(self, .Out_Ok, ^bool)
				if out_ok != nil do out_ok^ = true
			}
			return
		},
		complete = proc(self: ^Case, ok: bool) {
			out_ok := get(self, .Out_Ok, ^bool)
			if out_ok != nil do out_ok^ = ok
		},
		subscribe = proc(self: ^Case, handle: Handle, case_idx: int) {
			sem := get(self, .Id, Semaphore)
			inner := get_inner(sem)
			queue.enqueue(&inner.waiters, Waiter{handle, case_idx})
		},
		unsubscribe = proc(self: ^Case, handle: Handle) {
			sem := get(self, .Id, Semaphore)
			inner := get_inner(sem)
			for _ in 0 ..< queue.len(inner.waiters) {
				waiter := queue.pop_front(&inner.waiters)
				if waiter.handle != handle do queue.enqueue(&inner.waiters, waiter)
			}
		},
	}
}

@(deferred_in_out = _guard)
guard :: proc(self: Semaphore, cancel: Maybe(Cancel_Token) = nil) -> bool {
	return acquire(self, cancel)
}

@(private = "file")
_guard :: proc(self: Semaphore, _: Maybe(Cancel_Token), ok: bool) {
	if ok do release(self)
}

@(private = "file")
try_get_inner :: proc(self: Semaphore) -> (inner: ^Inner_Semaphore, ok: bool) {
	sched := get_scheduler()
	res := storage.get_ptr(&sched.resources, u64(self)) or_return
	return (^Inner_Semaphore)(res.ud[0]), true
}

@(private = "file")
get_inner :: proc(self: Semaphore) -> ^Inner_Semaphore {
	inner, ok := try_get_inner(self)
	assert(ok, "invalid semaphore")
	return inner
}

