package scheduler

import "base:runtime"
import "core:container/queue"
import "core:mem"
import "core:time"

import "../coro"
import "../storage"
import tw "../time_wheel"

User_Data :: struct {
	ctx:    runtime.Context,
	sched:  ^Scheduler,
	co:     ^coro.Coro,
	fn:     proc(arg: rawptr),
	arg:    rawptr,
	id:     u64,
	queued: bool,
}

Waker :: struct {
	sched: ^Scheduler,
	id:    u64,
}

wake :: proc(self: Waker) {
	ud, ok := storage.get(&self.sched.slots, self.id)
	assert(ok, "invalid task id")

	if !(ud^).queued {
		(ud^).queued = true
		queue.enqueue(&self.sched.ready, self.id)
	}
}

Scheduler :: struct {
	slots:      storage.Storage(^User_Data),
	ready:      queue.Queue(u64),
	sleeping:   storage.Storage(Waker),
	time_wheel: tw.Time_Wheel,
	finished:   [dynamic]tw.Task,
}

init :: proc(self: ^Scheduler) {
	storage.init(&self.slots, 1024)
	queue.init(&self.ready)
	storage.init(&self.sleeping, 1024)

	tw.init(&self.time_wheel, 1 * time.Millisecond)
	self.finished = make([dynamic]tw.Task)
}

deinit :: proc(self: ^Scheduler) {
	assert(storage.count(&self.slots) == 0, "scheduler has pending tasks")

	storage.deinit(&self.slots)
	queue.destroy(&self.ready)
	storage.deinit(&self.sleeping)

	tw.deinit(&self.time_wheel)
	delete(self.finished)
}

poll :: proc(self: ^Scheduler) {
	for queue.len(self.ready) > 0 {
		task_id := queue.pop_front(&self.ready)
		ud, ok := storage.get(&self.slots, task_id)
		assert(ok, "invalid task")

		(ud^).queued = false
		co := (ud^).co
		coro.check(coro.resume(co))

		if coro.status(co) == .Dead {
			storage.remove(&self.slots, task_id)
			ud := (^User_Data)(coro.get_user_data(co))
			free(ud)
			coro.check(coro.destroy(co))
		}
	}

	tw.spin(&self.time_wheel, &self.finished)
	if len(self.finished) > 0 {
		for id in self.finished {
			waker, ok := storage.remove(&self.sleeping, id)
			assert(ok, "invalid task")
			wake(waker)
		}
	}
	clear(&self.finished)
}

spawn :: proc(
	self: ^Scheduler,
	fn: proc(arg: rawptr),
	arg: rawptr = nil,
	stack_size: uint = 64 * mem.Kilobyte,
) {
	entry := storage.entry(&self.slots)

	ud := new(User_Data)
	ud.ctx = context
	ud.sched = self
	ud.co = new(coro.Coro)
	ud.fn = fn
	ud.id = storage.get_id(&entry)
	ud.arg = arg

	storage.insert(&entry, ud)

	raw_fn := proc "c" (co: ^coro.Coro) {
		ud := (^User_Data)(coro.get_user_data(co))
		context = ud.ctx

		arg := ud.arg
		ud.arg = nil

		ud.fn(arg)
	}

	desc := coro.desc_init(raw_fn, stack_size)
	desc.user_data = ud
	desc.storage_size = 0

	coro.check(coro.create(&ud.co, &desc))
	queue.enqueue(&self.ready, ud.id)
}

sleep :: proc(n: time.Duration) {
	sched := get_instance()
	waker := get_waker(sched)

	id := storage.add(&sched.sleeping, waker)
	tw.after(&sched.time_wheel, n, tw.Task(id))

	yield()
}

yield :: #force_inline proc() {
	coro.check(coro.yield(coro.running()))
}

@(private)
get_user_data :: #force_inline proc() -> ^User_Data {
	return (^User_Data)(coro.get_user_data(coro.running()))
}

get_instance :: #force_inline proc() -> ^Scheduler {
	return get_user_data().sched
}

get_waker :: #force_inline proc(self: ^Scheduler) -> Waker {
	ud := get_user_data()
	return {self, ud.id}
}

get_pending :: #force_inline proc(self: ^Scheduler) -> uint {
	return storage.count(&self.slots)
}

