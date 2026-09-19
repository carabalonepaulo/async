package async

import "base:runtime"
import "core:testing"
import "storage"

@(private = "file")
Inner_One_Shot :: struct {
	value:    rawptr,
	handle:   Maybe(Handle),
	case_idx: int,
}

One_Shot :: struct($T: typeid) {
	id:      u64,
	_marker: [0]T,
}

create_one_shot :: proc($T: typeid) -> One_Shot(T) {
	res := Resource{}
	res.id = auto_cast Internal_Resource.One_Shot

	inner := resource_as_inner(&res)
	inner.value = nil

	sched := get_scheduler()
	id := storage.add(&sched.resources, res)
	return One_Shot(T){id = id}
}

one_shot_destroy :: proc(self: One_Shot($T)) {
	sched := get_scheduler()
	res, ok := storage.remove(&sched.resources, self.id)
	assert(ok, "invalid one shot")

	inner := resource_as_inner(&res)
	if handle, handle_ok := inner.handle.(Handle); handle_ok {
		if inner.case_idx == -1 do wake(handle)
		else do wake_case(handle, inner.case_idx, false)
	}
	if inner.value != nil do free((^T)(inner.value), allocator = runtime.default_allocator())
}

one_shot_try_send :: proc(self: One_Shot($T), value: T) -> (ok: bool) {
	inner := try_get_inner(self) or_return
	if inner.value != nil do return false

	inner.value = new(T, allocator = runtime.default_allocator())
	(^T)(inner.value)^ = value

	if handle, handle_ok := inner.handle.(Handle); handle_ok {
		if inner.case_idx == -1 do wake(handle)
		else do wake_case(handle, inner.case_idx, true)
	}

	return true
}

one_shot_send :: proc(self: One_Shot($T), value: T) {
	ok := one_shot_try_send(self, value)
	assert(ok, "One_Shot can only be used once")
}

one_shot_try_recv :: proc(self: One_Shot($T)) -> (value: T, ok: bool) {
	inner := try_get_inner(self) or_return
	if inner.value == nil do return {}, false

	value = (^T)(inner.value)^
	ok = true
	one_shot_destroy(self)
	return
}

one_shot_recv :: proc(self: One_Shot($T)) -> (value: T, ok: bool) {
	inner := get_inner(self)

	if inner.value != nil {
		value = (^T)(inner.value)^
		ok = true
		one_shot_destroy(self)
		return
	}

	inner.handle = get_handle()
	inner.case_idx = -1

	yield()

	inner = try_get_inner(self) or_return
	inner.handle = nil
	inner.case_idx = 0

	value = (^T)(inner.value)^
	ok = true

	one_shot_destroy(self)
	return
}

one_shot_branch :: proc(self: One_Shot($T), out: ^T, out_ok: ^bool) -> Case {
	User_Data :: enum {
		Id,
		Out,
		Out_Ok,
	}

	get :: #force_inline proc(self: ^Case, ud: User_Data, $A: typeid) -> A {
		return transmute(A)(self.ud[ud])
	}

	get_one_shot :: #force_inline proc(self: ^Case) -> One_Shot(T) {
		return One_Shot(T){id = get(self, .Id, u64)}
	}

	return Case {
		ud = [MAX_USER_DATA]rawptr{transmute(rawptr)(self.id), out, out_ok, nil, nil},
		is_alive = proc(self: ^Case) -> bool {
			id := get(self, .Id, u64)
			sched := get_scheduler()
			_, ok := storage.get_ptr(&sched.resources, id)
			return ok
		},
		try = proc(self: ^Case) -> (ok: bool) {
			os := get_one_shot(self)
			value := one_shot_try_recv(os) or_return
			get(self, .Out, ^T)^ = value
			get(self, .Out_Ok, ^bool)^ = true
			return true
		},
		complete = proc(self: ^Case, ok: bool) {
			get(self, .Out_Ok, ^bool)^ = ok
			if ok {
				os := get_one_shot(self)
				inner := get_inner(os)
				get(self, .Out, ^T)^ = (^T)(inner.value)^
				one_shot_destroy(os)
			}
		},
		subscribe = proc(self: ^Case, handle: Handle, case_idx: int) {
			os := get_one_shot(self)
			inner := get_inner(os)
			assert(inner.handle == nil)
			inner.handle = handle
			inner.case_idx = case_idx
		},
		unsubscribe = proc(self: ^Case, handle: Handle) {
			os := get_one_shot(self)
			inner := get_inner(os)
			inner.handle = nil
			inner.case_idx = 0
		},
	}
}

@(private = "file")
try_get_inner :: proc(self: One_Shot($T)) -> (inner: ^Inner_One_Shot, ok: bool) {
	sched := get_scheduler()
	res := storage.get_ptr(&sched.resources, self.id) or_return
	return resource_as_inner(res), true
}

@(private = "file")
get_inner :: proc(self: One_Shot($T)) -> ^Inner_One_Shot {
	inner, ok := try_get_inner(self)
	assert(ok, "invalid one shot")
	return inner
}

@(private = "file")
resource_as_inner :: proc(res: ^Resource) -> ^Inner_One_Shot {
	#assert(size_of([MAX_USER_DATA]rawptr) >= size_of(Inner_One_Shot))
	#assert(align_of([MAX_USER_DATA]rawptr) >= align_of(Inner_One_Shot))
	return transmute(^Inner_One_Shot)(&res.ud[0])
}

@(test)
test_regression :: proc(t: ^testing.T) {
	init()
	defer deinit()

	primary := spawn(t, proc(t: ^testing.T) {
		os := create_one_shot(int)
		secondary := spawn(os, proc(os: One_Shot(int)) {
			send(os, 123)
		})

		value, ok := recv(os)
		testing.expect(t, ok)
		testing.expect(t, value == 123)
	})

	block(primary)
}

