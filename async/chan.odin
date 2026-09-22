package async

import "base:runtime"
import "core:container/queue"

import "storage"

@(private = "file")
Waiter :: struct {
	handle:   Handle,
	dest_ptr: rawptr,
	case_idx: int,
}

@(private)
Result :: struct($T: typeid) {
	value: T,
	ok:    bool,
}

@(private)
Inner_Chan :: struct($T: typeid) {
	receivers: queue.Queue(Waiter),
	items:     queue.Queue(Result(T)),
}

Chan :: struct($T: typeid) {
	id:      u64,
	_marker: [0]T,
}

create_chan :: proc($T: typeid, cap := 16) -> Chan(T) {
	inner := new(Inner_Chan(T))
	queue.init(&inner.receivers, 1)
	queue.init(&inner.items, cap)

	sched := get_scheduler()
	res := Resource{}
	res.id = auto_cast Internal_Resource.Channel
	res.ud[0] = inner
	id := storage.add(&sched.resources, res)
	return Chan(T){id = id}
}

chan_destroy :: proc(self: Chan($T)) {
	sched := get_scheduler()
	res, ok := storage.remove(&sched.resources, self.id)
	if !ok do return

	inner := (^Inner_Chan(T))(res.ud[0])
	for inner.receivers.len > 0 {
		waiter := queue.pop_front(&inner.receivers)
		if waiter.case_idx == -1 do send(waiter.handle, Result(T){ok = false})
		else do wake_case(waiter.handle, waiter.case_idx, false)
	}

	assert(inner.items.len == 0, "channel destroyed with unconsumed buffered items (leak)")

	queue.destroy(&inner.receivers)
	queue.destroy(&inner.items)

	free(inner)
}

chan_try_send :: proc(self: Chan($T), value: T) -> bool {
	inner := get_inner(self)
	if inner == nil do return false

	sched := get_scheduler()

	for inner.receivers.len > 0 {
		waiter := queue.pop_front(&inner.receivers)

		if waiter.case_idx == -1 {
			send(waiter.handle, Result(T){value, true})
			return true
		}

		if wake_case(waiter.handle, waiter.case_idx, true) {
			(^T)(waiter.dest_ptr)^ = value
			return true
		}
	}

	return false
}

chan_send :: proc(self: Chan($T), value: T) {
	if !chan_try_send(self, value) {
		queue.enqueue(&get_inner(self).items, Result(T){value, true})
	}
}

chan_try_recv :: proc(self: Chan($T)) -> (T, bool) {
	inner := get_inner(self)
	if inner == nil do return {}, false

	if inner.items.len > 0 {
		result := queue.pop_front(&inner.items)
		return result.value, result.ok
	}

	return {}, false
}

chan_recv :: proc(self: Chan($T)) -> (T, bool) {
	inner := get_inner(self)
	if inner == nil do return {}, false

	if inner.items.len > 0 {
		result := queue.pop_front(&inner.items)
		return result.value, result.ok
	}

	waiter := Waiter {
		handle   = get_handle(),
		dest_ptr = nil,
		case_idx = -1,
	}

	queue.enqueue(&inner.receivers, waiter)
	result := recv(Result(T))
	return result.value, result.ok
}

clear :: proc(self: Chan($T), destroy_item: Maybe(proc(item: ^T)) = nil) {
	inner := get_inner(self)
	if inner == nil do return

	for queue.len(inner.items) > 0 {
		result := queue.pop_front(&inner.items)
		if fn, ok := destroy_item.(proc(item: ^T)); ok {
			if result.ok do fn(&result.value)
		}
	}
}

len :: #force_inline proc(self: Chan($T)) -> int {
	inner := get_inner(self)
	return inner == nil ? 0 : queue.len(inner.items)
}

chan_branch :: proc(ch: Chan($T), out: ^T = nil, out_ok: ^bool = nil) -> Case {
	User_Data :: enum {
		Id,
		Receivers,
		Out,
		Out_Ok,
	}

	get :: #force_inline proc(self: ^Case, idx: User_Data, $T: typeid) -> T {
		return transmute(T)(self.ud[idx])
	}

	id: u64 = storage.INVALID
	chan: ^Inner_Chan(T)
	receivers: ^queue.Queue(Waiter)

	if inner := get_inner(ch); inner != nil {
		id = ch.id
		chan = inner
		receivers = &inner.receivers
	}

	ud := [MAX_USER_DATA]rawptr{}
	ud[User_Data.Id] = transmute(rawptr)(id)
	ud[User_Data.Receivers] = receivers
	ud[User_Data.Out] = out
	ud[User_Data.Out_Ok] = out_ok

	return Case {
		ud = ud, //
		is_alive = proc(self: ^Case) -> bool {
			id := get(self, .Id, u64)
			return is_chan_alive(id)
		},
		try = proc(self: ^Case) -> bool {
			id := get(self, .Id, u64)
			out := get(self, .Out, ^T)
			out_ok := get(self, .Out_Ok, ^bool)

			ch := Chan(T){id, {}}
			sched := get_scheduler()
			inner := get_inner(ch)
			if inner.items.len > 0 {
				result := queue.pop_front(&inner.items)
				if out != nil do (^T)(out)^ = result.value
				if out_ok != nil do out_ok^ = result.ok
				return true
			}

			return false
		},
		complete = proc(self: ^Case, ok: bool) {
			out_ok := get(self, .Out_Ok, ^bool)
			if out_ok != nil do out_ok^ = ok
		},
		subscribe = proc(self: ^Case, handle: Handle, case_idx: int) {
			waiter := Waiter {
				handle   = handle,
				dest_ptr = get(self, .Out, ^T),
				case_idx = case_idx,
			}
			receivers := get(self, .Receivers, ^queue.Queue(Waiter))
			queue.enqueue(receivers, waiter)
		},
		unsubscribe = proc(self: ^Case, handle: Handle) {
			receivers := get(self, .Receivers, ^queue.Queue(Waiter))
			size := receivers.len
			for _ in 0 ..< size {
				waiter := queue.pop_front(receivers)
				if waiter.handle != handle do queue.enqueue(receivers, waiter)
			}
		},
	}
}

@(private)
is_chan_alive :: proc {
	is_chan_alive_by_handle,
	is_chan_alive_by_id,
}

@(private)
is_chan_alive_by_id :: proc(id: u64) -> bool {
	sched := get_scheduler()
	_, ok := storage.get_ptr(&sched.resources, id)
	return ok
}

@(private)
is_chan_alive_by_handle :: #force_inline proc(chan: Chan($T)) -> bool {
	return is_chan_alive_by_id(chan.id)
}

@(private)
get_inner :: proc(chan: Chan($T)) -> ^Inner_Chan(T) {
	sched := get_scheduler()
	res, ok := storage.get_ptr(&sched.resources, chan.id)
	if !ok do return nil
	return (^Inner_Chan(T))(res.ud[0])
}

chan_into_rawptr :: #force_inline proc(self: Chan($T)) -> rawptr {
	return transmute(rawptr)(self.id)
}

chan_from_rawptr :: #force_inline proc($T: typeid, ptr: rawptr) -> Chan(T) {
	return Chan(T){id = transmute(u64)(ptr)}
}

