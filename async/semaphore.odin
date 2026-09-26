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

	res := Resource {
		id = auto_cast Internal_Resource.Semaphore,
	}

	inner := load_inline(&res.ud, Inner_Semaphore)
	inner.n = n
	inner.cap = n
	queue.init(&inner.waiters)

	return Semaphore(add_resource(res))
}

semaphore_destroy :: proc(self: Semaphore) {
	sched := get_scheduler()
	res, ok := try_remove_resource(u64(self))
	assert(ok, "invalid semaphore")

	inner := load_inline(&res.ud, Inner_Semaphore)
	inner.n = 0

	for queue.len(inner.waiters) > 0 {
		waiter := queue.pop_front(&inner.waiters)
		if waiter.case_idx == -1 do wake(waiter.handle)
		else do wake_case(waiter.handle, waiter.case_idx, false)
	}

	queue.destroy(&inner.waiters)
}

try_acquire :: proc(self: Semaphore) -> bool {
	inner := get_inner(self)
	return _try_acquire(inner)
}

acquire :: proc(self: Semaphore, cancel: Maybe(Cancel_Token) = nil) -> (ok: bool) {
	inner := get_inner(self)
	if _try_acquire(inner) do return true

	if cancel, cancel_ok := cancel.(Cancel_Token); cancel_ok {
		idx := select({branch(cancel), branch(self, &ok)})
		return idx == 0 ? false : ok
	}

	queue.enqueue(&inner.waiters, Waiter{get_handle(), -1})
	yield()
	inner = try_get_inner(self) or_return
	return _try_acquire(inner)
}

release :: proc(self: Semaphore) {
	inner := get_inner(self)
	assert(inner.n < inner.cap)
	inner.n += 1

	if waiter, ok := queue.pop_front_safe(&inner.waiters); ok {
		if waiter.case_idx == -1 do wake(waiter.handle)
		else do wake_case(waiter.handle, waiter.case_idx, true)
	}
}

semaphore_branch :: proc(self: Semaphore, out_ok: ^bool) -> Case {
	Case_State :: struct {
		sem:    Semaphore,
		out_ok: ^bool,
	}

	ud := [CASE_INLINE_STORAGE]rawptr{}
	store_inline(&ud, Case_State{self, out_ok})

	return Case {
		ud = ud, //
		is_alive = proc(self: ^Case) -> bool {
			state := load_inline(&self.ud, Case_State)
			sched := get_scheduler()
			_, ok := storage.get_ptr(&sched.resources, transmute(u64)(state.sem))
			return ok
		},
		try = proc(self: ^Case) -> (ok: bool) {
			state := load_inline(&self.ud, Case_State)
			if ok = try_acquire(state.sem); ok {
				if state.out_ok != nil do state.out_ok^ = true
			}
			return
		},
		complete = proc(self: ^Case, ok: bool) {
			state := load_inline(&self.ud, Case_State)
			inner := get_inner(state.sem)
			if state.out_ok != nil do state.out_ok^ = ok && _try_acquire(inner)
		},
		subscribe = proc(self: ^Case, handle: Handle, case_idx: int) {
			state := load_inline(&self.ud, Case_State)
			inner := get_inner(state.sem)
			queue.enqueue(&inner.waiters, Waiter{handle, case_idx})
		},
		unsubscribe = proc(self: ^Case, handle: Handle) {
			state := load_inline(&self.ud, Case_State)
			inner := get_inner(state.sem)
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
	return load_inline(&res.ud, Inner_Semaphore), true
}

@(private = "file")
get_inner :: proc(self: Semaphore) -> ^Inner_Semaphore {
	inner, ok := try_get_inner(self)
	assert(ok, "invalid semaphore")
	return inner
}

@(private = "file")
_try_acquire :: proc(inner: ^Inner_Semaphore) -> bool {
	if inner.n > 0 {
		inner.n -= 1
		return true
	}
	return false
}

