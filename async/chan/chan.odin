package chan

import "core:container/queue"
import "core:time"

import sch "../scheduler"

@(private)
Case :: struct {
	ch:        rawptr,
	receivers: ^queue.Queue(Waiter),
	pop:       proc(ch: rawptr, dest: rawptr) -> bool,
	ptr:       rawptr,
}

@(private)
Select :: struct {
	cases:    []Case,
	resolved: bool,
}

@(private)
Waiter :: struct {
	handle:   sch.Handle,
	dest_ptr: rawptr,
	select:   ^Select,
	case_idx: int,
	done:     bool,
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

init :: proc(self: ^Chan($T), cap := 16) {
	queue.init(&self.receivers, 1)
	queue.init(&self.items, cap)
	self.open = true
}

deinit :: proc(self: ^Chan($T)) {
	for self.receivers.len > 0 {
		waiter := queue.pop_front(&self.receivers)
		sch.send(waiter.handle, Result(T){ok = false})
	}

	assert(self.items.len == 0)

	queue.destroy(&self.receivers)
	queue.destroy(&self.items)

	self.open = false
}

send :: proc(self: ^Chan($T), value: T) {
	assert(self.open)

	for self.receivers.len > 0 {
		waiter := queue.pop_front(&self.receivers)

		if waiter.select != nil {
			if waiter.select.resolved do continue
			waiter.select.resolved = true

			ptr := (^T)(waiter.dest_ptr)
			ptr^ = value

			sch.send(waiter.handle, waiter.case_idx)
			return
		}

		sch.send(waiter.handle, Result(T){value, true})
		return
	}

	queue.enqueue(&self.items, Result(T){value, true})
}

recv :: proc(self: ^Chan($T)) -> (T, bool) {
	if self.items.len > 0 {
		result := queue.pop_front(&self.items)
		return result.value, result.ok
	}

	waiter := Waiter {
		handle   = sch.get_handle(),
		dest_ptr = nil,
		select   = nil,
		done     = false,
	}

	queue.enqueue(&self.receivers, waiter)
	result := sch.recv(Result(T))
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

	state := Select {
		cases    = cases,
		resolved = false,
	}

	handle := sch.get_handle()
	for c, i in cases {
		waiter := Waiter {
			handle   = handle,
			dest_ptr = c.ptr,
			select   = &state,
			case_idx = i,
		}
		queue.enqueue(c.receivers, waiter)
	}

	idx := sch.recv(int)
	for c in cases do remove_waiter(c.receivers, handle)
	return idx
}

@(private)
remove_waiter :: proc(q: ^queue.Queue(Waiter), handle: sch.Handle) {
	size := q.len
	for _ in 0 ..< size {
		waiter := queue.pop_front(q)
		if waiter.handle.id != handle.id do queue.enqueue(q, waiter)
	}
}

