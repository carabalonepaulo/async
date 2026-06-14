package scheduler

import "base:runtime"
import "core:container/queue"
import "core:mem"
import "core:time"

import "../coro"
import "../storage"

Sleep :: struct {
	target_time: time.Time,
	waker:       Waker,
}

User_Data :: struct {
	ctx:     runtime.Context,
	sched:   ^Scheduler,
	co:      ^coro.Coro,
	fn:      proc(arg: rawptr),
	arg:     rawptr,
	task_id: u64,
	queued:  bool,
}

Waker :: struct {
	sched:   ^Scheduler,
	task_id: u64,
}

wake :: proc(self: Waker) {
	ud, ok := storage.get(&self.sched.slots, self.task_id)
	assert(ok, "invalid task id")

	if !(ud^).queued {
		(ud^).queued = true
		queue.enqueue(&self.sched.ready, self.task_id)
	}
}

Scheduler :: struct {
	slots:    storage.Storage(^User_Data),
	ready:    queue.Queue(u64),
	sleeping: storage.Storage(Sleep),
}

init :: proc(self: ^Scheduler) {
	storage.init(&self.slots, 1024)
	queue.init(&self.ready)
	storage.init(&self.sleeping, 1024)
}

deinit :: proc(self: ^Scheduler) {
	count := 0
	iter := storage.iter(&self.slots)
	for _, _ in storage.iterate(&iter) do count += 1
	assert(count == 0, "scheduler has pending tasks")

	storage.deinit(&self.slots)
	queue.destroy(&self.ready)
	storage.deinit(&self.sleeping)
}

poll :: proc(self: ^Scheduler) {
	for queue.len(self.ready) > 0 {
		task_id := queue.pop_front(&self.ready)
		ud, ok := storage.get(&self.slots, task_id)
		assert(ok, "invalid task")

		(ud^).queued = false
		co := (ud^).co
		coro.resume(co)

		if coro.status(co) == .Dead {
			storage.remove(&self.slots, task_id)
			ud := (^User_Data)(coro.get_user_data(co))
			free(ud)
			coro.destroy(co)
		}
	}

	now := time.now()
	iter := storage.iter(&self.sleeping)
	for id, sleep in storage.iterate(&iter) {
		if time.diff(now, sleep.target_time) <= 0 {
			wake(sleep.waker)
			storage.remove(&self.sleeping, id)
		}
	}
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
	ud.task_id = storage.get_id(&entry)
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

	coro.check(coro.create(&ud.co, &desc))
	queue.enqueue(&self.ready, ud.task_id)
}

sleep :: proc(n: time.Duration) {
	sched := get_instance()
	waker := get_waker(sched)
	target := time.time_add(time.now(), n)
	storage.add(&sched.sleeping, Sleep{target, waker})
	coro.check(coro.yield(coro.running()))
}

get_user_data :: #force_inline proc() -> ^User_Data {
	return (^User_Data)(coro.get_user_data(coro.running()))
}

get_waker :: #force_inline proc(self: ^Scheduler) -> Waker {
	ud := get_user_data()
	return {self, ud.task_id}
}

get_pending :: #force_inline proc(self: ^Scheduler) -> uint {
	return storage.count(&self.slots)
}

get_instance :: #force_inline proc() -> ^Scheduler {
	return get_user_data().sched
}

