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

@(private = "file")
Inner_Inline_One_Shot :: struct($T: typeid) {
	value:    Maybe(T),
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

	sched := get_scheduler()
	id := storage.add(&sched.resources, res)
	return One_Shot(T){id = id}
}

one_shot_destroy :: proc(self: One_Shot($T)) {
	sched := get_scheduler()
	res, ok := storage.remove(&sched.resources, self.id)
	assert(ok, "invalid one shot")

	raw_inner: rawptr
	when (size_of(Inner_Inline_One_Shot(T)) <= RESOURCE_INLINE_STORAGE * size_of(rawptr) &&
		align_of(Inner_Inline_One_Shot(T)) <= align_of(rawptr)) {
		raw_inner = load_inline(&res.ud, Inner_Inline_One_Shot(T))
	} else {
		inner := load_inline(&res.ud, Inner_One_Shot)
		if inner.value != nil do free((^T)(inner.value), allocator = runtime.default_allocator())
		raw_inner = inner
	}

	internal_wake(raw_inner, T, false)
}

one_shot_try_send :: proc(self: One_Shot($T), value: T) -> (ok: bool) {
	raw_inner: rawptr
	defer if ok do internal_wake(raw_inner, T, true)

	when (size_of(Inner_Inline_One_Shot(T)) <= RESOURCE_INLINE_STORAGE * size_of(rawptr) &&
		align_of(Inner_Inline_One_Shot(T)) <= align_of(rawptr)) {
		inner := try_get_inner(self, Inner_Inline_One_Shot(T)) or_return
		if _, ok := inner.value.(T); ok do return false
		raw_inner = inner

		inner.value = value
	} else {
		inner := try_get_inner(self, Inner_One_Shot) or_return
		if inner.value != nil do return false
		raw_inner = inner

		inner.value = new(T, allocator = runtime.default_allocator())
		(^T)(inner.value)^ = value
	}

	return true
}

one_shot_send :: proc(self: One_Shot($T), value: T) {
	ok := one_shot_try_send(self, value)
	assert(ok, "One_Shot can only be used once")
}

one_shot_try_recv :: proc(self: One_Shot($T)) -> (value: T, ok: bool) {
	defer if ok do one_shot_destroy(self)

	raw_inner := try_get_raw_inner(self) or_return
	value, ok = get_value(raw_inner, T)

	return
}

one_shot_recv :: proc(self: One_Shot($T)) -> (value: T, ok: bool) {
	defer if ok do one_shot_destroy(self)

	raw_inner := try_get_raw_inner(self) or_return
	value, ok = get_value(raw_inner, T)
	if ok do return

	set_both(raw_inner, T, get_handle(), -1, false)
	yield()
	_ = storage.get_ptr(&get_scheduler().resources, self.id) or_return
	set_both(raw_inner, T, nil, 0, false)

	value, ok = get_value(raw_inner, T)

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

	ud := [CASE_INLINE_STORAGE]rawptr{}
	ud[User_Data.Id] = transmute(rawptr)(self.id)
	ud[User_Data.Out] = out
	ud[User_Data.Out_Ok] = out_ok

	return Case {
		ud = ud, //
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
				raw_inner := get_raw_inner(os)
				get(self, .Out, ^T)^ = get_value(raw_inner, T)
				one_shot_destroy(os)
			}
		},
		subscribe = proc(self: ^Case, handle: Handle, case_idx: int) {
			os := get_one_shot(self)
			raw_inner := get_raw_inner(os)
			set_both(raw_inner, T, handle, case_idx, true)
		},
		unsubscribe = proc(self: ^Case, handle: Handle) {
			os := get_one_shot(self)
			raw_inner := get_raw_inner(os)
			set_both(raw_inner, T, nil, 0, false)
		},
	}
}

is_inline :: proc(self: One_Shot($T)) -> bool {
	return(
		size_of(Inner_Inline_One_Shot(T)) <= RESOURCE_INLINE_STORAGE * size_of(rawptr) &&
		align_of(Inner_Inline_One_Shot(T)) <= align_of(rawptr) \
	)
}

@(private = "file")
try_get_inner :: proc(self: One_Shot($T), $O: typeid) -> (inner: ^O, ok: bool) {
	sched := get_scheduler()
	res := storage.get_ptr(&sched.resources, self.id) or_return
	return load_inline(&res.ud, O), true
}

@(private = "file")
try_get_raw_inner :: proc(self: One_Shot($T)) -> (raw: rawptr, ok: bool) #optional_ok {
	when (size_of(Inner_Inline_One_Shot(T)) <= RESOURCE_INLINE_STORAGE * size_of(rawptr) &&
		align_of(Inner_Inline_One_Shot(T)) <= align_of(rawptr)) {
		return try_get_inner(self, Inner_Inline_One_Shot(T))
	} else {
		return try_get_inner(self, Inner_One_Shot)
	}
}

@(private = "file")
get_raw_inner :: proc(self: One_Shot($T)) -> rawptr {
	ptr, ok := try_get_raw_inner(self)
	assert(ok, "invalid one shot")
	return ptr
}

@(private = "file")
get_both :: proc(raw_inner: rawptr, $T: typeid) -> (^Maybe(Handle), ^int) {
	when (size_of(Inner_Inline_One_Shot(T)) <= RESOURCE_INLINE_STORAGE * size_of(rawptr) &&
		align_of(Inner_Inline_One_Shot(T)) <= align_of(rawptr)) {
		inner := (^Inner_Inline_One_Shot(T))(raw_inner)
		return &inner.handle, &inner.case_idx
	} else {
		inner := (^Inner_One_Shot)(raw_inner)
		return &inner.handle, &inner.case_idx
	}
}

@(private = "file")
get_value :: proc(raw_inner: rawptr, $T: typeid) -> (value: T, ok: bool) #optional_ok {
	when (size_of(Inner_Inline_One_Shot(T)) <= RESOURCE_INLINE_STORAGE * size_of(rawptr) &&
		align_of(Inner_Inline_One_Shot(T)) <= align_of(rawptr)) {
		inner := (^Inner_Inline_One_Shot(T))(raw_inner)
		value, ok = inner.value.(T)
		return
	} else {
		inner := (^Inner_One_Shot)(raw_inner)
		if inner.value == nil do return {}, false
		return (^T)(inner.value)^, true
	}
}

@(private = "file")
set_both :: proc(
	inner: rawptr,
	$T: typeid,
	handle: Maybe(Handle),
	case_idx: int,
	$ensure_no_handle: bool,
) {
	when (size_of(Inner_Inline_One_Shot(T)) <= RESOURCE_INLINE_STORAGE * size_of(rawptr) &&
		align_of(Inner_Inline_One_Shot(T)) <= align_of(rawptr)) {
		inner := (^Inner_Inline_One_Shot(T))(inner)
		when ensure_no_handle do assert(inner.handle == nil)
		inner.handle = handle
		inner.case_idx = case_idx
	} else {
		inner := (^Inner_One_Shot)(inner)
		inner.handle = handle
		inner.case_idx = case_idx
	}
}

@(private = "file")
internal_wake :: #force_inline proc(self: rawptr, $T: typeid, ok: bool) {
	handle, case_idx := get_both(self, T)
	if handle, handle_ok := (handle^).(Handle); handle_ok {
		if case_idx^ == -1 do wake(handle)
		else do wake_case(handle, case_idx^, ok)
	}
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

