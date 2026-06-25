package async

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

Chan :: struct($T: typeid) {
	receivers: queue.Queue(Waiter),
	items:     queue.Queue(Result(T)),
	id:        u64,
	sched:     ^Scheduler,
}

Chan_Handle :: distinct u64

chan_init :: proc(sched: ^Scheduler, self: ^Chan($T), cap := 16) {
	queue.init(&self.receivers, 1)
	queue.init(&self.items, cap)
	self.id = storage.add(&sched.channels, true)
	self.sched = sched
}

chan_init_from_coro :: proc(self: ^Chan($T), cap := 16) {
	sched := get_instance()
	chan_init(sched, self, cap)
}

chan_deinit :: proc(self: ^Chan($T)) {
	storage.remove(&self.sched.channels, self.id)

	for self.receivers.len > 0 {
		waiter := queue.pop_front(&self.receivers)
		if waiter.case_idx == -1 {
			send(waiter.handle, Result(T){ok = false})
		} else {
			ud, ok := storage.get(&waiter.handle.sched.slots, waiter.handle.id)
			if !ok do continue
			if coro.get_bytes_stored(ud.co) > 0 do continue
			send(waiter.handle, -(waiter.case_idx + 1))
		}
	}

	assert(self.items.len == 0, "channel destroyed with unconsumed buffered items (leak)")

	queue.destroy(&self.receivers)
	queue.destroy(&self.items)
}

chan_send :: proc(self: ^Chan($T), value: T) {
	assert(is_chan_alive(self), "cannot send to a closed or uninitialized channel")

	for self.receivers.len > 0 {
		waiter := queue.pop_front(&self.receivers)

		if waiter.case_idx == -1 {
			send(waiter.handle, Result(T){value, true})
			return
		}

		ud, ok := storage.get(&waiter.handle.sched.slots, waiter.handle.id)
		if !ok do continue
		if coro.get_bytes_stored(ud.co) > 0 do continue

		ptr := (^T)(waiter.dest_ptr)
		ptr^ = value
		send(waiter.handle, waiter.case_idx)
		return
	}

	queue.enqueue(&self.items, Result(T){value, true})
}

chan_recv :: proc(self: ^Chan($T)) -> (T, bool) {
	if self.items.len > 0 {
		result := queue.pop_front(&self.items)
		return result.value, result.ok
	}

	waiter := Waiter {
		handle   = get_handle(),
		dest_ptr = nil,
		case_idx = -1,
	}

	queue.enqueue(&self.receivers, waiter)
	result := recv(Result(T))
	return result.value, result.ok
}

drain :: proc(self: ^Chan($T)) -> (T, bool) {
	assert(queue.len(self.receivers) == 0, "channel has active coroutines waiting to receive data")
	if queue.len(self.items) > 0 {
		result := queue.pop_front(&self.items)
		return result.value, result.ok
	}
	return {}, false
}

clear :: proc(self: ^Chan($T), destroy_item: Maybe(proc(item: ^T)) = nil) {
	for queue.len(self.items) > 0 {
		result := queue.pop_front(&self.items)
		if fn, ok := destroy_item.(proc(item: ^T)); ok {
			if result.ok do fn(&result.value)
		}
	}
}

len :: #force_inline proc(self: ^Chan($T)) -> int {
	return queue.len(self.items)
}

default_branch :: proc(ch: ^Chan($T), out: ^T = nil, out_ok: ^bool = nil) -> Case {
	id: u64 = storage.INVALID
	chan: ^Chan(T)
	receivers: ^queue.Queue(Waiter)

	if is_chan_alive(ch) {
		id = ch.id
		chan = ch
		receivers = &ch.receivers
	}

	return Case {
		ch_id = id,
		ch = chan,
		receivers = receivers,
		out_ptr = out,
		out_ok_ptr = out_ok,
		pop = proc(raw_ch: rawptr, out: rawptr, out_ok: ^bool) -> bool {
			ch := (^Chan(T))(raw_ch)
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
	sched := get_instance()

	for c, i in cases {
		if !is_chan_alive(sched, c.ch_id) {
			if c.out_ok_ptr != nil do c.out_ok_ptr^ = false
			return i
		}
		if c.pop(c.ch, c.out_ptr, c.out_ok_ptr) do return i
	}
	if timeout == 0 do return -1

	handle := get_handle()
	timer_id := storage.add(&handle.sched.sleeping, handle)
	tw.after(&handle.sched.time_wheel, timeout, tw.Task(timer_id))

	for c, i in cases {
		waiter := Waiter {
			handle   = handle,
			dest_ptr = c.out_ptr,
			case_idx = i,
		}
		queue.enqueue(c.receivers, waiter)
	}

	yield()

	ud := get_user_data()
	idx: int

	if coro.get_bytes_stored(ud.co) >= size_of(int) {
		raw_idx := pop(int)
		storage.remove(&handle.sched.sleeping, timer_id)

		if raw_idx < 0 {
			idx = (-raw_idx) - 1
			for c, i in cases do if i != idx do remove_waiter(c.receivers, handle)
			return idx
		} else do idx = raw_idx
		if cases[idx].out_ok_ptr != nil do cases[idx].out_ok_ptr^ = raw_idx >= 0
	} else {
		idx = -1
	}

	for c in cases do remove_waiter(c.receivers, handle)
	return idx
}

@(private)
remove_waiter :: proc(q: ^queue.Queue(Waiter), handle: Handle) {
	size := q.len
	for _ in 0 ..< size {
		waiter := queue.pop_front(q)
		if waiter.handle.id != handle.id do queue.enqueue(q, waiter)
	}
}

@(private)
is_chan_alive :: proc {
	is_chan_alive_by_ref,
	is_chan_alive_by_id,
}

@(private)
is_chan_alive_by_ref :: #force_inline proc(self: ^Chan($T)) -> bool {
	if self == nil do return false
	return is_chan_alive_by_id(self.sched, self.id)
}

@(private)
is_chan_alive_by_id :: #force_inline proc(sched: ^Scheduler, chan_id: u64) -> bool {
	_, ok := storage.get_ptr(&sched.channels, chan_id)
	return ok
}

