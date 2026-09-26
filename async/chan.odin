package async

import "core:container/queue"
import "core:fmt"
import "core:testing"

CHAN_INITIAL_CAPACITY :: 16

@(private = "file")
Waiter :: struct {
	handle:   Handle,
	case_idx: int,
}

@(private = "file")
Inner_Chan :: struct($T: typeid) {
	waiters: queue.Queue(Waiter),
	items:   queue.Queue(T),
}

Chan :: struct($T: typeid) {
	id:      u64,
	_marker: [0]T,
}

create_chan :: proc($T: typeid, cap: int = CHAN_INITIAL_CAPACITY) -> Chan(T) {
	res := Resource {
		id = auto_cast Internal_Resource.Channel,
	}

	inner := load_inline(&res.ud, Inner_Chan(T))
	queue.init(&inner.waiters)
	queue.init(&inner.items, cap)

	id := add_resource(res)
	return Chan(T){id = id}
}

chan_destroy :: proc(self: Chan($T)) {
	res, ok := try_remove_resource(self.id)
	assert(ok, "attempt to destroy invalid channel")

	inner := load_inline(&res.ud, Inner_Chan(T))
	for waiter in queue.pop_front_safe(&inner.waiters) {
		if waiter.case_idx == -1 do wake(waiter.handle)
		else do wake_case(waiter.handle, waiter.case_idx, false)
	}
	queue.destroy(&inner.waiters)
	queue.destroy(&inner.items)
}

chan_try_send :: proc(self: Chan($T), value: T) -> (ok: bool) {
	inner := get_inner(self)
	if waiter, ok := queue.pop_front_safe(&inner.waiters); ok {
		queue.enqueue(&inner.items, value)
		if waiter.case_idx == -1 do wake(waiter.handle)
		else do wake_case(waiter.handle, waiter.case_idx, true)
		return true
	}
	return false
}

chan_send :: proc(self: Chan($T), value: T) {
	inner := get_inner(self)
	queue.enqueue(&inner.items, value)

	if waiter, ok := queue.pop_front_safe(&inner.waiters); ok {
		if waiter.case_idx == -1 do wake(waiter.handle)
		else do wake_case(waiter.handle, waiter.case_idx, true)
	}
}

chan_try_recv :: proc(self: Chan($T)) -> (value: T, ok: bool) {
	inner := try_get_inner(self) or_return
	return queue.pop_front_safe(&inner.items)
}

chan_recv :: proc(self: Chan($T)) -> (value: T, ok: bool) {
	{
		inner := try_get_inner(self) or_return
		if queue.len(inner.items) == 0 {
			queue.enqueue(&inner.waiters, Waiter{get_handle(), -1})
			yield()
		}
	}

	inner := try_get_inner(self) or_return
	return queue.pop_front_safe(&inner.items)
}

chan_clear :: proc(self: Chan($T), destroy_item: Maybe(proc(item: ^T)) = nil) {
	inner := get_inner(self)
	if fn, ok := destroy_item.(proc(item: ^T)); ok {
		for {
			item := queue.pop_front_safe(&inner.items) or_break
			fn(&item)
		}
	} else do queue.clear(&inner.items)
}

chan_len :: proc(self: Chan($T)) -> int {
	inner, ok := try_get_inner(self)
	return ok ? queue.len(&inner.items) : 0
}

chan_branch :: proc(self: Chan($T), out: ^T = nil, out_ok: ^bool = nil) -> Case {
	State :: struct {
		ch:     Chan(T),
		out:    ^T,
		out_ok: ^bool,
	}

	ud := [CASE_INLINE_STORAGE]rawptr{}
	state := load_inline(&ud, State)
	state^ = State{self, out, out_ok}

	return Case {
		ud = ud,
		//
		is_alive = proc(self: ^Case) -> bool {
			state := load_inline(&self.ud, State)
			_, ok := try_get_resource(state.ch.id)
			return ok
		},
		try = proc(self: ^Case) -> bool {
			state := load_inline(&self.ud, State)
			inner := get_inner(state.ch)

			if item, ok := queue.pop_front_safe(&inner.items); ok {
				if state.out != nil do state.out^ = item
				if state.out_ok != nil do state.out_ok^ = true
				return true
			}

			return false
		},
		complete = proc(self: ^Case, ok: bool) {
			state := load_inline(&self.ud, State)
			inner := get_inner(state.ch)

			if ok {
				value, value_ok := queue.pop_front_safe(&inner.items)
				if state.out != nil do state.out^ = value
				if state.out_ok != nil do state.out_ok^ = value_ok
			} else if state.out_ok != nil do state.out_ok^ = false
		},
		subscribe = proc(self: ^Case, handle: Handle, case_idx: int) {
			state := load_inline(&self.ud, State)
			inner := get_inner(state.ch)
			queue.enqueue(&inner.waiters, Waiter{handle, case_idx})
		},
		unsubscribe = proc(self: ^Case, handle: Handle) {
			state := load_inline(&self.ud, State)
			inner := get_inner(state.ch)
			size := queue.len(inner.waiters)

			for _ in 0 ..< size {
				waiter := queue.pop_front(&inner.waiters)
				if waiter.handle != handle do queue.enqueue(&inner.waiters, waiter)
			}
		},
	}
}

chan_into_rawptr :: #force_inline proc(self: Chan($T)) -> rawptr {
	return transmute(rawptr)(self.id)
}

chan_from_rawptr :: #force_inline proc($T: typeid, ptr: rawptr) -> Chan(T) {
	return Chan(T){id = transmute(u64)(ptr)}
}

@(private = "file")
try_get_inner :: proc(self: Chan($T)) -> (inner: ^Inner_Chan(T), ok: bool) {
	res := try_get_resource(self.id) or_return
	return load_inline(&res.ud, Inner_Chan(T)), true
}

@(private = "file")
get_inner :: proc(self: Chan($T), loc := #caller_location) -> ^Inner_Chan(T) {
	inner, ok := try_get_inner(self)
	assert(ok, fmt.tprintf("invalid chan at %v", loc))
	return inner
}

@(test)
test_normal :: proc(t: ^testing.T) {
	init()
	defer deinit()

	ch := create_chan(int)
	defer chan_destroy(ch)

	VALUE :: 123

	producer :: proc(t: ^testing.T, ch: Chan(int)) {
		chan_send(ch, VALUE)
	}
	a := spawn(t, ch, producer)

	consumer :: proc(t: ^testing.T, ch: Chan(int)) {
		value, ok := chan_recv(ch)
		testing.expect(t, ok)
		testing.expect(t, value == VALUE)
	}
	b := spawn(t, ch, consumer)

	block(spawn([]Handle{a, b}, proc(handles: []Handle) {
			join_many(handles)
		}))
}

@(test)
test_fail_to_recv :: proc(t: ^testing.T) {
	init()
	defer deinit()

	ch := create_chan(int)

	consumer :: proc(t: ^testing.T, ch: Chan(int)) {
		value, ok := chan_recv(ch)
		testing.expect(t, ok == false)
		testing.expect(t, value == 0)
	}
	a := spawn(t, ch, consumer)

	block(spawn(a, ch, proc(a: Handle, ch: Chan(int)) {
			chan_destroy(ch)
			join(a)
		}))
}

@(test)
test_try_recv :: proc(t: ^testing.T) {
	init()
	defer deinit()

	VALUE :: 123

	ch := create_chan(int)
	defer chan_destroy(ch)

	chan_send(ch, VALUE)

	value, ok := chan_try_recv(ch)
	testing.expect(t, ok)
	testing.expect(t, value == VALUE)
}

@(test)
test_try_send_success :: proc(t: ^testing.T) {
	init()
	defer deinit()

	VALUE :: 123

	ch := create_chan(int)
	defer chan_destroy(ch)

	consumer :: proc(t: ^testing.T, ch: Chan(int)) {
		value, ok := chan_recv(ch)
		testing.expect(t, ok)
		testing.expect(t, value == VALUE)
	}
	a := spawn(t, ch, consumer)

	producer :: proc(t: ^testing.T, ch: Chan(int)) {
		ok := chan_try_send(ch, VALUE)
		testing.expect(t, ok)
	}
	b := spawn(t, ch, producer)

	block(spawn([]Handle{a, b}, proc(handles: []Handle) {
			join_many(handles)
		}))
}

@(test)
test_try_send_fail :: proc(t: ^testing.T) {
	init()
	defer deinit()

	VALUE :: 123

	ch := create_chan(int)
	defer chan_destroy(ch)

	producer :: proc(t: ^testing.T, ch: Chan(int)) {
		ok := chan_try_send(ch, VALUE)
		testing.expect(t, ok == false)
	}
	a := spawn(t, ch, producer)

	block(spawn(a, proc(a: Handle) {join(a)}))
}

@(test)
test_chan_select_alone_win :: proc(t: ^testing.T) {
	init()
	defer deinit()

	VALUE :: 123

	ch := create_chan(int)
	defer chan_destroy(ch)

	consumer :: proc(t: ^testing.T, ch: Chan(int)) {
		value: int
		ok: bool

		idx := select({chan_branch(ch, &value, &ok)})
		testing.expect(t, idx == 0)
		testing.expect(t, ok)
		testing.expect(t, value == VALUE)
	}
	a := spawn(t, ch, consumer)

	producer :: proc(ch: Chan(int)) {
		chan_send(ch, VALUE)
	}
	b := spawn(ch, producer)

	block(spawn([]Handle{a, b}, proc(handles: []Handle) {
			join_many(handles)
		}))
}

