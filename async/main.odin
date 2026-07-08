package async

import "base:builtin"
import "base:runtime"
import "core:c"
import "core:container/queue"
import "core:mem"
import "core:time"

import "coro"
import "storage"
import tw "time_wheel"

INITIAL_CAPACITY :: #config(ASYNC_INITIAL_CAPACITY, 1024)
MAX_USER_DATA :: #config(ASYNC_MAX_USER_DATA, 5)

Next_Tick :: struct {
	ud: rawptr,
	fn: proc(ud: rawptr),
}

Timer :: struct {
	fn: proc(ud: rawptr),
	ud: rawptr,
}

Internal_State :: struct {
	ctx:       runtime.Context,
	co:        ^coro.Coro,
	fn:        rawptr,
	id:        u64,
	queued:    bool,
	allocator: mem.Allocator,
	ud:        [MAX_USER_DATA]rawptr,
}

Handle :: distinct u64

wake :: proc(self: Handle) {
	ud, ok := storage.get(&scheduler.slots, u64(self))
	assert(ok, "invalid task id")

	if !ud.queued {
		ud.queued = true
		queue.enqueue(&scheduler.ready, u64(self))
	}
}

scheduler_send :: proc(self: Handle, value: $T) {
	ud, ok := storage.get(&scheduler.slots, u64(self))
	assert(ok, "invalid task id")

	if !ud.queued {
		push(ud.co, value)
		ud.queued = true
		queue.enqueue(&scheduler.ready, u64(self))
	} else {
		panic("multiple send before recv")
	}
}

Scheduler :: struct {
	next_tick:            queue.Queue(Next_Tick),
	slots:                storage.Storage(^Internal_State),
	ready:                queue.Queue(u64),
	timers:               storage.Storage(Timer),
	channels:             storage.Storage(rawptr),
	active_cancel_tokens: map[u64]bool,
	time_wheel:           tw.Time_Wheel,
	finished:             [dynamic]tw.Task,
}

@(thread_local)
scheduler: Scheduler

scheduler_init :: proc() {
	queue.init(&scheduler.next_tick)
	storage.init(&scheduler.slots, INITIAL_CAPACITY)
	queue.init(&scheduler.ready)
	storage.init(&scheduler.timers, INITIAL_CAPACITY)
	storage.init(&scheduler.channels, INITIAL_CAPACITY)

	scheduler.active_cancel_tokens = make(map[u64]bool)

	tw.init(&scheduler.time_wheel, 1 * time.Millisecond)
	scheduler.finished = make([dynamic]tw.Task)
}

scheduler_deinit :: proc() {
	destroy_cancel_tokens()

	for queue.len(scheduler.next_tick) > 0 {
		meta := queue.dequeue(&scheduler.next_tick)
		meta.fn(meta.ud)
	}
	queue.destroy(&scheduler.next_tick)

	assert(storage.count(&scheduler.slots) == 0, "scheduler has pending tasks")
	assert(storage.count(&scheduler.channels) == 0, "scheduler has active channels")

	storage.deinit(&scheduler.slots)
	queue.destroy(&scheduler.ready)
	storage.deinit(&scheduler.timers)
	storage.deinit(&scheduler.channels)

	tw.deinit(&scheduler.time_wheel)
	delete(scheduler.finished)
}

scheduler_run :: proc(sleep: time.Duration = 0) {
	for get_pending() > 0 {
		poll()
		if sleep > 0 do time.sleep(sleep)
	}
}

scheduler_run_with :: proc(tick: proc(), sleep: time.Duration = 0) {
	for get_pending() > 0 {
		tick()
		poll()
		if sleep > 0 do time.sleep(sleep)
	}
}

scheduler_run_with_poly :: proc(arg: $T, tick: proc(arg: T), sleep: time.Duration = 0) {
	for get_pending() > 0 {
		tick(arg)
		poll()
		if sleep > 0 do time.sleep(sleep)
	}
}

poll :: proc() {
	len := queue.len(scheduler.next_tick)
	for _ in 0 ..< len {
		meta := queue.dequeue(&scheduler.next_tick)
		meta.fn(meta.ud)
	}

	for queue.len(scheduler.ready) > 0 {
		task_id := queue.pop_front(&scheduler.ready)
		ud, ok := storage.get(&scheduler.slots, task_id)
		assert(ok, "invalid task")

		ud.queued = false
		coro.check(coro.resume(ud.co))

		if coro.status(ud.co) == .Dead {
			storage.remove(&scheduler.slots, task_id)
			coro.check(coro.destroy(ud.co))
			free(ud)
		}
	}

	tw.spin(&scheduler.time_wheel, &scheduler.finished)
	if builtin.len(scheduler.finished) > 0 {
		for id in scheduler.finished {
			if task, ok := storage.remove(&scheduler.timers, id); ok {
				task.fn(task.ud)
			}
		}
	}
	runtime.clear(&scheduler.finished)
}

spawn_with_data :: proc(
	arg: $T,
	fn: proc(arg: T),
	stack_size: uint = 64 * mem.Kilobyte,
	storage_size: uint = 256,
	stack_allocator := context.allocator,
) -> Handle {
	arg := arg

	ud := create_ud(rawptr(fn), stack_allocator)
	raw_fn := proc "c" (co: ^coro.Coro) {
		ud := (^Internal_State)(coro.get_user_data(co))
		context = ud.ctx
		((proc(arg: T))(ud.fn))(pop(T))
	}

	desc := create_desc(raw_fn, ud, stack_size, storage_size)
	coro.check(coro.create(&ud.co, &desc))
	coro.push(ud.co, &arg, size_of(T))

	queue.enqueue(&scheduler.ready, ud.id)

	return Handle(ud.id)
}

spawn_without_data :: proc(
	fn: proc(),
	stack_size: uint = 64 * mem.Kilobyte,
	storage_size: uint = 256,
	stack_allocator := context.allocator,
) -> Handle {
	ud := create_ud(rawptr(fn), stack_allocator)
	raw_fn := proc "c" (co: ^coro.Coro) {
		ud := (^Internal_State)(coro.get_user_data(co))
		context = ud.ctx
		((proc())(ud.fn))()
	}

	desc := create_desc(raw_fn, ud, stack_size, storage_size)
	coro.check(coro.create(&ud.co, &desc))
	queue.enqueue(&scheduler.ready, ud.id)

	return Handle(ud.id)
}

next_tick :: proc(fn: proc(ud: rawptr), ud: rawptr = nil) {
	queue.enqueue(&scheduler.next_tick, Next_Tick{ud, fn})
}

timer :: proc(n: time.Duration, fn: proc(ud: rawptr), ud: rawptr = nil) -> u64 {
	id := storage.add(&scheduler.timers, Timer{fn, ud})
	tw.after(&scheduler.time_wheel, n, tw.Task(id))
	return id
}

sleep :: proc(n: time.Duration) {
	ud := get_internal_state()
	fn := proc(ud: rawptr) {wake(Handle(transmute(u64)(ud)))}
	timer(n, fn, transmute(rawptr)(ud.id))
	yield()
}

reschedule :: #force_inline proc() {
	wake(get_handle())
	yield()
}

yield :: #force_inline proc() {
	coro.check(coro.yield(coro.running()))
}

scheduler_recv :: #force_inline proc($T: typeid) -> T {
	yield()
	return pop(T)
}

@(private)
get_internal_state :: #force_inline proc() -> ^Internal_State {
	return (^Internal_State)(coro.get_user_data(coro.running()))
}

get_user_data_from_current :: proc(idx: int) -> rawptr {
	return get_internal_state().ud[idx]
}

get_user_data_from_handle :: proc(handle: Handle, idx: int) -> rawptr {
	state, ok := storage.get(&scheduler.slots, u64(handle))
	return ok ? state.ud[idx] : nil
}

set_user_data_to_current :: proc(idx: int, ud: rawptr) {
	get_internal_state().ud[idx] = ud
}

set_user_data_to_handle :: proc(handle: Handle, idx: int, ud: rawptr) {
	state, ok := storage.get(&scheduler.slots, u64(handle))
	if ok do state.ud[idx] = ud
}

get_scheduler :: #force_inline proc() -> ^Scheduler {
	return &scheduler
}

get_handle :: #force_inline proc() -> Handle {
	ud := get_internal_state()
	return Handle(ud.id)
}

get_pending :: #force_inline proc() -> uint {
	return storage.count(&scheduler.slots)
}

@(private)
push :: proc(co: ^coro.Coro, value: $T) {
	value := value
	coro.check(coro.push(co, &value, size_of(T)))
}

@(private)
pop :: proc($T: typeid) -> T {
	ud := get_internal_state()
	if coro.get_bytes_stored(ud.co) < size_of(T) do panic("send/recv mismatch")
	value: T
	coro.check(coro.pop(ud.co, &value, size_of(T)))
	return value
}

@(private)
create_ud :: proc(fn: rawptr, allocator: mem.Allocator) -> ^Internal_State {
	entry := storage.entry(&scheduler.slots)

	ud := new(Internal_State)
	ud.ctx = context
	ud.co = new(coro.Coro)
	ud.fn = fn
	ud.id = storage.get_id(&entry)
	ud.allocator = allocator

	storage.insert(&entry, ud)

	return ud
}

@(private)
create_desc :: proc(
	raw_fn: proc "c" (co: ^coro.Coro),
	ud: ^Internal_State,
	stack_size: uint,
	storage_size: uint,
) -> (
	desc: coro.Desc,
) {
	desc = coro.desc_init(raw_fn, stack_size)
	desc.user_data = ud
	desc.storage_size = storage_size
	desc.allocator_data = ud
	desc.alloc_cb = proc "c" (size: c.size_t, allocator_data: rawptr) -> rawptr {
		ud := (^Internal_State)(allocator_data)
		context = ud.ctx
		ptr, _ := mem.alloc(int(size), allocator = ud.allocator)
		return ptr
	}
	desc.dealloc_cb = proc "c" (ptr: rawptr, size: c.size_t, allocator_data: rawptr) {
		ud := (^Internal_State)(allocator_data)
		context = ud.ctx
		mem.free_with_size(ptr, int(size), allocator = ud.allocator)
	}
	return
}

handle_into_rawptr :: #force_inline proc(handle: Handle) -> rawptr {
	return transmute(rawptr)(handle)
}

handle_from_rawptr :: #force_inline proc(ptr: rawptr) -> Handle {
	return transmute(Handle)(ptr)
}

