package scheduler

import "base:runtime"
import "core:container/queue"
import "core:mem"
import "core:time"

import "../coro"
import "../storage"
import tw "../time_wheel"

INITIAL_CAPACITY :: #config(ASYNC_INITIAL_CAPACITY, 1024)

User_Data :: struct {
	ctx:    runtime.Context,
	sched:  ^Scheduler,
	co:     ^coro.Coro,
	fn:     proc(),
	arg:    rawptr,
	id:     u64,
	queued: bool,
}

Handle :: struct {
	sched: ^Scheduler,
	id:    u64,
}

wake :: proc(self: Handle) {
	ud, ok := storage.get(&self.sched.slots, self.id)
	assert(ok, "invalid task id")

	if !ud.queued {
		ud.queued = true
		queue.enqueue(&self.sched.ready, self.id)
	}
}

send :: proc(self: Handle, value: $T) {
	ud, ok := storage.get(&self.sched.slots, self.id)
	assert(ok, "invalid task id")

	if !ud.queued {
		push(ud.co, value)
		ud.queued = true
		queue.enqueue(&self.sched.ready, self.id)
	} else {
		panic("multiple send before recv")
	}
}

Scheduler :: struct {
	slots:      storage.Storage(^User_Data),
	ready:      queue.Queue(u64),
	sleeping:   storage.Storage(Handle),
	time_wheel: tw.Time_Wheel,
	finished:   [dynamic]tw.Task,
}

init :: proc(self: ^Scheduler) {
	storage.init(&self.slots, INITIAL_CAPACITY)
	queue.init(&self.ready)
	storage.init(&self.sleeping, INITIAL_CAPACITY)

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

		ud.queued = false
		coro.check(coro.resume(ud.co))

		if coro.status(ud.co) == .Dead {
			storage.remove(&self.slots, task_id)
			coro.check(coro.destroy(ud.co))
			free(ud)
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

spawn :: proc {
	spawn_with_data,
	spawn_without_data,
}

spawn_with_data :: proc(
	self: ^Scheduler,
	arg: $T,
	fn: proc(arg: T),
	stack_size: uint = 64 * mem.Kilobyte,
	storage_size: uint = 256,
) -> Handle {
	entry := storage.entry(&self.slots)
	arg := arg

	ud := new(User_Data)
	ud.ctx = context
	ud.sched = self
	ud.co = new(coro.Coro)
	ud.id = storage.get_id(&entry)
	ud.arg = rawptr(fn)

	storage.insert(&entry, ud)

	raw_fn := proc "c" (co: ^coro.Coro) {
		ud := (^User_Data)(coro.get_user_data(co))
		context = ud.ctx

		fn := (proc(arg: T))(ud.arg)
		ud.arg = nil

		fn(pop(T))
	}

	desc := coro.desc_init(raw_fn, stack_size)
	desc.user_data = ud
	desc.storage_size = storage_size

	coro.check(coro.create(&ud.co, &desc))
	coro.push(ud.co, &arg, size_of(T))

	queue.enqueue(&self.ready, ud.id)

	return Handle{self, ud.id}
}

spawn_without_data :: proc(
	self: ^Scheduler,
	fn: proc(),
	stack_size: uint = 64 * mem.Kilobyte,
	storage_size: uint = 256,
) -> Handle {
	entry := storage.entry(&self.slots)

	ud := new(User_Data)
	ud.ctx = context
	ud.sched = self
	ud.co = new(coro.Coro)
	ud.fn = fn
	ud.id = storage.get_id(&entry)

	storage.insert(&entry, ud)

	raw_fn := proc "c" (co: ^coro.Coro) {
		ud := (^User_Data)(coro.get_user_data(co))
		context = ud.ctx
		ud.fn()
	}

	desc := coro.desc_init(raw_fn, stack_size)
	desc.user_data = ud
	desc.storage_size = storage_size

	coro.check(coro.create(&ud.co, &desc))
	queue.enqueue(&self.ready, ud.id)

	return Handle{self, ud.id}
}

sleep :: proc(n: time.Duration) {
	ud := get_user_data()
	id := storage.add(&ud.sched.sleeping, Handle{ud.sched, ud.id})
	tw.after(&ud.sched.time_wheel, n, tw.Task(id))
	yield()
}

reschedule :: #force_inline proc() {
	wake(get_handle())
	yield()
}

yield :: #force_inline proc() {
	coro.check(coro.yield(coro.running()))
}

recv :: #force_inline proc($T: typeid) -> T {
	yield()
	return pop(T)
}

@(private)
get_user_data :: #force_inline proc() -> ^User_Data {
	return (^User_Data)(coro.get_user_data(coro.running()))
}

get_instance :: #force_inline proc() -> ^Scheduler {
	return get_user_data().sched
}

get_handle :: #force_inline proc() -> Handle {
	ud := get_user_data()
	return {ud.sched, ud.id}
}

get_pending :: #force_inline proc(self: ^Scheduler) -> uint {
	return storage.count(&self.slots)
}

@(private)
push :: proc(co: ^coro.Coro, value: $T) {
	value := value
	coro.check(coro.push(co, &value, size_of(T)))
}

@(private)
pop :: proc($T: typeid) -> T {
	ud := get_user_data()
	if coro.get_bytes_stored(ud.co) < size_of(T) do panic("send/recv mismatch")
	value: T
	coro.check(coro.pop(ud.co, &value, size_of(T)))
	return value
}

