package async

import "storage"

@(private = "file")
Inner_Wait_Group :: struct {
	count:  int,
	waiter: Maybe(Handle),
}

Wait_Group :: distinct u64

create_wait_group :: proc() -> Wait_Group {
	sched := get_scheduler()
	id := storage.add(&sched.resources, Resource{})
	return Wait_Group(id)
}

wait_group_destroy :: proc(self: Wait_Group) {
	state := get_inner(self)
	assert(state.waiter == nil && state.count == 0, "destroying active wait group")

	sched := get_scheduler()
	storage.remove(&sched.resources, transmute(u64)(self))
}

add :: proc(self: Wait_Group, n: int = 1) {
	assert(n > 0, "wait group add must be positive")
	get_inner(self).count += n
}

done :: proc(self: Wait_Group) {
	state := get_inner(self)
	assert(state.count > 0, "wait group count is already zero")

	state.count -= 1
	if state.count == 0 {
		if waiter, ok := state.waiter.(Handle); ok {
			wake(waiter)
		}
	}
}

wait_group_wait :: proc(self: Wait_Group) {
	state := get_inner(self)
	assert(state.waiter == nil, "wait group can only have a single waiter")

	if state.count == 0 do return
	state.waiter = get_handle()
	yield()
	state.waiter = nil
}

@(private = "file")
get_inner :: proc(self: Wait_Group) -> ^Inner_Wait_Group {
	sched := get_scheduler()
	res, ok := storage.get_ptr(&sched.resources, transmute(u64)(self))
	assert(ok, "invalid wait group")
	return load_inline(&res.ud, Inner_Wait_Group)
}

