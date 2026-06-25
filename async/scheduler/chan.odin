package scheduler

import "async:storage"
import "core:container/queue"
import "core:time"

import "../coro"
// import sch "../scheduler"
import tw "../time_wheel"

@(private)
Case :: struct {
	ch:        rawptr,
	receivers: ^queue.Queue(Waiter),
	pop:       proc(ch: rawptr, dest: rawptr) -> bool,
	ptr:       rawptr,
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
	open:      bool,
}

chan_init :: proc(self: ^Chan($T), cap := 16) {
	queue.init(&self.receivers, 1)
	queue.init(&self.items, cap)
	self.open = true
}

chan_deinit :: proc(self: ^Chan($T)) {
	for self.receivers.len > 0 {
		waiter := queue.pop_front(&self.receivers)
		send(waiter.handle, Result(T){ok = false})
	}

	assert(self.items.len == 0)

	queue.destroy(&self.receivers)
	queue.destroy(&self.items)

	self.open = false
}

chan_send :: proc(self: ^Chan($T), value: T) {
	assert(self.open)

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

branch :: proc(ch: ^Chan($T), out: ^T) -> Case {
	return Case {
		ch = ch,
		receivers = &ch.receivers,
		ptr = out,
		pop = proc(raw_ch: rawptr, dest: rawptr) -> bool {
			ch := (^Chan(T))(raw_ch)
			if ch.items.len > 0 {
				result := queue.pop_front(&ch.items)
				(^T)(dest)^ = result.value
				return true
			}
			return false
		},
	}
}

select :: proc(cases: []Case, timeout: time.Duration = time.Duration(0)) -> int {
	for c, i in cases do if c.pop(c.ch, c.ptr) do return i
	if timeout <= 0 do return -1

	handle := get_handle()
	timer_id := storage.add(&handle.sched.sleeping, handle)
	tw.after(&handle.sched.time_wheel, timeout, tw.Task(timer_id))

	for c, i in cases {
		waiter := Waiter {
			handle   = handle,
			dest_ptr = c.ptr,
			case_idx = i,
		}
		queue.enqueue(c.receivers, waiter)
	}

	yield()

	ud := get_user_data()
	idx: int

	if coro.get_bytes_stored(ud.co) >= size_of(int) {
		idx = pop(int)
		storage.remove(&handle.sched.sleeping, timer_id)
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

