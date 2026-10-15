package async

@(private = "file")
Inner_Wait_Group :: struct {
	count:   uint,
	waiters: Waiters,
}

Wait_Group :: distinct u64

create_wait_group :: proc() -> Wait_Group {
	sched := get_scheduler()

	res := Resource {
		id = auto_cast Internal_Resource.Wait_Group,
	}
	inner := load_inline(&res.ud, Inner_Wait_Group)
	init_waiters(&inner.waiters)

	id := add_resource(res)
	return Wait_Group(id)
}

wait_group_destroy :: proc(self: Wait_Group) {
	res, ok := try_remove_resource(transmute(u64)(self))
	assert(ok, "invalid wait group")

	inner := load_inline(&res.ud, Inner_Wait_Group)
	deinit_waiters(&inner.waiters)
}

add :: proc(self: Wait_Group, n: uint = 1) {
	get_inner(self).count += n
}

done :: proc(self: Wait_Group) {
	inner := get_inner(self)
	assert(inner.count > 0, "wait group count is already zero")

	inner.count -= 1
	if inner.count == 0 {
		for handle, case_idx in try_remove_waiter(&inner.waiters) {
			wake_waiter(handle, case_idx, true)
		}
	}
}

wait_group_wait :: proc(self: Wait_Group) -> (ok: bool) {
	inner := try_get_inner(self) or_return
	if inner.count == 0 do return true

	add_waiter(&inner.waiters, get_handle())
	yield()
	return has_resource(transmute(u64)(self))
}

wait_group_branch :: proc(self: Wait_Group, ok: ^bool = nil) -> Case {
	State :: struct {
		wg:     Wait_Group,
		waiter: Waiter,
		out_ok: ^bool,
	}

	ud := [CASE_INLINE_STORAGE]rawptr{}
	load_inline(&ud, State)^ = State {
		wg     = self,
		out_ok = ok,
	}

	return Case {
		ud = ud, //
		is_alive = proc(self: ^Case) -> bool {
			state := load_inline(&self.ud, State)
			return has_resource(transmute(u64)(state.wg))
		},
		try = proc(self: ^Case) -> (ok: bool) {
			state := load_inline(&self.ud, State)
			inner := get_inner(state.wg)
			return inner.count == 0
		},
		complete = proc(self: ^Case, ok: bool) {
			state := load_inline(&self.ud, State)
			if state.out_ok != nil do state.out_ok^ = ok
		},
		subscribe = proc(self: ^Case, handle: Handle, case_idx: int) {
			state := load_inline(&self.ud, State)
			inner := get_inner(state.wg)
			state.waiter = add_waiter(&inner.waiters, handle, case_idx)
		},
		unsubscribe = proc(self: ^Case, handle: Handle) {
			state := load_inline(&self.ud, State)
			inner := get_inner(state.wg)
			try_remove_waiter_by_id(&inner.waiters, state.waiter)
		},
	}
}

@(private = "file")
try_get_inner :: proc(self: Wait_Group) -> (inner: ^Inner_Wait_Group, ok: bool) {
	res := try_get_resource(transmute(u64)(self)) or_return
	return load_inline(&res.ud, Inner_Wait_Group), true
}

@(private = "file")
get_inner :: proc(self: Wait_Group) -> ^Inner_Wait_Group {
	inner, ok := try_get_inner(self)
	assert(ok, "invalid wait group")
	return inner
}

