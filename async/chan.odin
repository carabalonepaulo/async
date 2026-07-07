package async

import "base:builtin"
import "base:runtime"
import "core:container/queue"
import "core:fmt"
import "core:time"

import "coro"
import "storage"
import tw "time_wheel"

@(private)
Case :: struct {
	ch_id:      u64,
	ch:         rawptr,
	receivers:  ^queue.Queue(Waiter),
	pop:        proc(ch: rawptr, dest: rawptr, ok: ^bool) -> bool,
	out_ptr:    rawptr,
	out_ok_ptr: ^bool,
}

@(private)
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

Chan_Handle :: distinct u64

create_chan :: proc($T: typeid, cap := 16) -> Chan(T) {
	inner := new(Inner_Chan(T))
	queue.init(&inner.receivers, 1)
	queue.init(&inner.items, cap)

	sched := get_scheduler()
	id := storage.add(&sched.channels, rawptr(inner))
	return Chan(T){id = id}
}

chan_destroy :: proc(self: Chan($T)) {
	sched := get_scheduler()
	ptr, ok := storage.remove(&sched.channels, self.id)
	if !ok do return

	inner := (^Inner_Chan(T))(ptr)

	for inner.receivers.len > 0 {
		waiter := queue.pop_front(&inner.receivers)
		if waiter.case_idx == -1 {
			send(waiter.handle, Result(T){ok = false})
		} else {
			ud, ok := storage.get(&sched.slots, u64(waiter.handle))
			if !ok do continue
			if coro.get_bytes_stored(ud.co) > 0 do continue
			send(waiter.handle, -(waiter.case_idx + 1))
		}
	}

	assert(inner.items.len == 0, "channel destroyed with unconsumed buffered items (leak)")

	queue.destroy(&inner.receivers)
	queue.destroy(&inner.items)

	free(inner)
}

chan_try_send :: proc(self: Chan($T), value: T) -> bool {
	inner := get_inner(self)
	assert(inner != nil, "cannot send to a closed or uninitialized channel")

	sched := get_scheduler()

	for inner.receivers.len > 0 {
		waiter := queue.pop_front(&inner.receivers)

		if waiter.case_idx == -1 {
			send(waiter.handle, Result(T){value, true})
			return true
		}

		ud, ok := storage.get(&sched.slots, u64(waiter.handle))
		if !ok do continue
		if coro.get_bytes_stored(ud.co) > 0 do continue

		ptr := (^T)(waiter.dest_ptr)
		ptr^ = value
		send(waiter.handle, waiter.case_idx)
		return true
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
	assert(inner != nil, "cannot recv from a closed or uninitialized channel")

	if inner.items.len > 0 {
		result := queue.pop_front(&inner.items)
		return result.value, result.ok
	}

	return {}, false
}

chan_recv :: proc(self: Chan($T)) -> (T, bool) {
	inner := get_inner(self)
	assert(inner != nil, "cannot recv from a closed or uninitialized channel")

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

drain :: proc(self: Chan($T)) -> (T, bool) {
	inner := get_inner(self)
	assert(inner != nil, "cannot drain a closed or uninitialized channel")
	assert(
		queue.len(inner.receivers) == 0,
		"channel has active coroutines waiting to receive data",
	)

	if queue.len(inner.items) > 0 {
		result := queue.pop_front(&inner.items)
		return result.value, result.ok
	}
	return {}, false
}

clear :: proc(self: Chan($T), destroy_item: Maybe(proc(item: ^T)) = nil) {
	inner := get_inner(self)
	assert(inner != nil, "cannot clear a closed or uninitialized channel")

	for queue.len(inner.items) > 0 {
		result := queue.pop_front(&inner.items)
		if fn, ok := destroy_item.(proc(item: ^T)); ok {
			if result.ok do fn(&result.value)
		}
	}
}

len :: #force_inline proc(self: Chan($T)) -> int {
	inner := get_inner(self)
	assert(inner != nil, "cannot get length of a closed or uninitialized channel")
	return queue.len(inner.items)
}

default_branch :: proc(ch: Chan($T), out: ^T = nil, out_ok: ^bool = nil) -> Case {
	id: u64 = storage.INVALID
	chan: ^Inner_Chan(T)
	receivers: ^queue.Queue(Waiter)

	if inner := get_inner(ch); inner != nil {
		id = ch.id
		chan = inner
		receivers = &inner.receivers
	}

	return Case {
		ch_id = id,
		ch = chan,
		receivers = receivers,
		out_ptr = out,
		out_ok_ptr = out_ok,
		pop = proc(raw_ch: rawptr, out: rawptr, out_ok: ^bool) -> bool {
			ch := (^Inner_Chan(T))(raw_ch)
			if ch.items.len > 0 {
				result := queue.pop_front(&ch.items)
				if out != nil do (^T)(out)^ = result.value
				if out_ok != nil do out_ok^ = result.ok
				return true
			}
			return false
		},
	}
}

select :: proc(cases: []Case, timeout: time.Duration = -1) -> int {
	sched := get_scheduler()

	for c, i in cases {
		if !is_chan_alive(c.ch_id) {
			if c.out_ok_ptr != nil do c.out_ok_ptr^ = false
			return i
		}
		if c.pop(c.ch, c.out_ptr, c.out_ok_ptr) do return i
	}
	if timeout == 0 do return -1

	handle := get_handle()
	for c, i in cases {
		waiter := Waiter {
			handle   = handle,
			dest_ptr = c.out_ptr,
			case_idx = i,
		}
		queue.enqueue(c.receivers, waiter)
	}

	timer_id: u64
	if timeout > 0 {
		fn := proc(ud: rawptr) {wake(Handle(transmute(u64)(ud)))}
		timer_id = timer(timeout, fn, transmute(rawptr)(handle))
	}

	yield()

	ud := get_user_data()
	idx: int

	if coro.get_bytes_stored(ud.co) >= size_of(int) {
		raw_idx := pop(int)
		storage.remove(&sched.timers, timer_id)

		if raw_idx < 0 {
			idx = (-raw_idx) - 1
			for c, i in cases {
				if i != idx && is_chan_alive(c.ch_id) {
					remove_waiter(c.receivers, handle)
				}
			}
			return idx
		} else do idx = raw_idx
		if cases[idx].out_ok_ptr != nil do cases[idx].out_ok_ptr^ = raw_idx >= 0
	} else {
		idx = -1
	}

	for c in cases do if is_chan_alive(c.ch_id) do remove_waiter(c.receivers, handle)
	return idx
}

join :: proc(cases: []Case, timeout: time.Duration = -1) -> int {
	if builtin.len(cases) == 0 do return 0

	sched := get_scheduler()
	total := builtin.len(cases)

	active_cases := make([]Case, total)
	defer delete(active_cases)
	copy(active_cases, cases)

	remaining := total
	start_time := time.now()
	has_timeout := timeout >= 0
	time_left := timeout

	for remaining > 0 {
		if has_timeout && time_left <= 0 do break

		idx := select(active_cases[:remaining], timeout = time_left)
		if idx == -1 do break

		if idx >= 0 {
			remaining -= 1
			if idx < remaining do active_cases[idx] = active_cases[remaining]
		}

		if has_timeout do time_left = timeout - time.since(start_time)
	}

	return remaining
}

@(private)
remove_waiter :: proc(q: ^queue.Queue(Waiter), handle: Handle) {
	size := q.len
	for _ in 0 ..< size {
		waiter := queue.pop_front(q)
		if waiter.handle != handle do queue.enqueue(q, waiter)
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
	_, ok := storage.get(&sched.channels, id)
	return ok
}

@(private)
is_chan_alive_by_handle :: #force_inline proc(chan: Chan($T)) -> bool {
	return is_chan_alive_by_id(chan.id)
}

@(private)
get_inner :: proc(chan: Chan($T)) -> ^Inner_Chan(T) {
	sched := get_scheduler()
	ptr, ok := storage.get(&sched.channels, chan.id)
	if !ok do return nil
	return (^Inner_Chan(T))(ptr)
}

