package chan

import "core:container/queue"

import sch "../scheduler"

@(private)
Result :: struct($T: typeid) {
	value: T,
	ok:    bool,
}

Chan :: struct($T: typeid) {
	receivers: queue.Queue(sch.Handle),
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
		handle := queue.pop_front(&self.receivers)
		sch.send(handle, Result(T){ok = false})
	}

	assert(self.items.len == 0)

	queue.destroy(&self.receivers)
	queue.destroy(&self.items)

	self.open = false
}

send :: proc(self: ^Chan($T), value: T) {
	assert(self.open)

	if self.receivers.len > 0 {
		handle := queue.pop_front(&self.receivers)
		sch.send(handle, Result(T){value, true})
		return
	}

	queue.enqueue(&self.items, Result(T){value, true})
}

recv :: proc(self: ^Chan($T)) -> (T, bool) {
	if self.items.len > 0 {
		result := queue.pop_front(&self.items)
		return result.value, result.ok
	}

	handle := sch.get_handle()
	queue.enqueue(&self.receivers, handle)
	result := sch.recv(Result(T))
	return result.value, result.ok
}

