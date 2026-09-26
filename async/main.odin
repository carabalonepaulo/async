package async

import "base:builtin"
import "base:runtime"
import "core:c"
import "core:container/queue"
import "core:fmt"
import "core:mem"
import "core:time"

import "coro"
import "storage"
import tw "time_wheel"

INITIAL_CAPACITY :: #config(ASYNC_INITIAL_CAPACITY, 64)

RESOURCE_INLINE_STORAGE :: 16
CASE_INLINE_STORAGE :: 5

DEFAULT_STACK_SIZE :: #config(ASYNC_DEFAULT_STACK_SIZE, 64 * mem.Kilobyte)

Internal_Resource :: enum {
	Unknown,
	Coroutine,
	Timer,
	Channel,
	Cancel_Token,
	Semaphore,
	One_Shot,
}

Resource :: struct {
	id:   int,
	ud:   [RESOURCE_INLINE_STORAGE]rawptr,
	drop: proc(self: ^Resource),
}

@(private)
Hook :: enum {
	Exit,
}

@(private)
Closure :: struct {
	ud: rawptr,
	fn: proc(ud: rawptr),
}

@(private)
Internal_State :: struct {
	ctx:    runtime.Context,
	co:     ^coro.Coro,
	fn:     rawptr,
	id:     u64,
	queued: bool,
	hooks:  [Hook]Closure,
	winner: Maybe(int),
	args:   u64,
}

Handle :: distinct u64

Scheduler :: struct {
	next_tick:            queue.Queue(Closure),
	resources:            storage.Storage(Resource),
	ready:                queue.Queue(u64),
	active_cancel_tokens: map[u64]bool,
	active_coroutines:    uint,
	time_wheel:           tw.Time_Wheel,
	finished:             [dynamic]tw.Task,
}

@(thread_local)
scheduler: Scheduler

scheduler_init :: proc() {
	storage.init(&scheduler.resources, INITIAL_CAPACITY)
	queue.init(&scheduler.next_tick)
	queue.init(&scheduler.ready)

	scheduler.active_cancel_tokens = make(map[u64]bool)

	tw.init(&scheduler.time_wheel, 1 * time.Millisecond)
	scheduler.finished = make([dynamic]tw.Task)
}

scheduler_deinit :: proc() {
	for queue.len(scheduler.next_tick) > 0 {
		meta := queue.dequeue(&scheduler.next_tick)
		meta.fn(meta.ud)
	}
	queue.destroy(&scheduler.next_tick)

	storage.retain(&scheduler.resources, nil, proc(id: u64, res: ^Resource, ud: rawptr) -> bool {
		if res.drop != nil {
			res.drop(res)
			return false
		}
		fmt.printfln("resource leak: %v", res.id)
		return true
	})
	assert(storage.count(&scheduler.resources) == 0, "scheduler has active resources")

	queue.destroy(&scheduler.ready)
	storage.deinit(&scheduler.resources)

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

scheduler_block :: proc(handle: Handle, sleep: time.Duration = 0) {
	for {
		storage.get_ptr(&scheduler.resources, u64(handle)) or_break
		poll()
		if sleep > 0 do time.sleep(sleep)
	}
}

scheduler_block_with :: proc(handle: Handle, tick: proc(), sleep: time.Duration = 0) {
	for {
		storage.get_ptr(&scheduler.resources, u64(handle)) or_break
		tick()
		poll()
		if sleep > 0 do time.sleep(sleep)
	}
}

scheduler_block_with_poly :: proc(
	handle: Handle,
	a: $A,
	tick: proc(a: A),
	sleep: time.Duration = 0,
) {
	for {
		storage.get_ptr(&scheduler.slots, u64(handle)) or_break
		tick(a)
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

	ready_count := queue.len(scheduler.ready)
	for _ in 0 ..< ready_count {
		task_id := queue.pop_front(&scheduler.ready)
		res := storage.get(&scheduler.resources, task_id) or_continue

		ud := transmute(^Internal_State)(res.ud[0])
		ud.queued = false
		coro.check(coro.resume(ud.co))

		if coro.status(ud.co) == .Dead {
			storage.remove(&scheduler.resources, task_id)
			coro.check(coro.destroy(ud.co))
			free(ud)
			scheduler.active_coroutines -= 1
		}
	}

	tw.spin(&scheduler.time_wheel, &scheduler.finished)
	if builtin.len(scheduler.finished) > 0 {
		for id in scheduler.finished {
			if res, ok := storage.remove(&scheduler.resources, id); ok {
				closure := load_inline(&res.ud, Closure)
				closure.fn(closure.ud)
			}
		}
	}
	runtime.clear(&scheduler.finished)
}

join :: proc(handle: Handle) {
	state, ok := get_internal_state(handle)
	if !ok do return

	assert(state.hooks[.Exit].fn == nil, "multiple join calls on the same handle")
	state.hooks[.Exit] = Closure {
		ud = transmute(rawptr)(get_handle()),
		fn = auto_cast proc(handle: Handle) {wake(handle)},
	}
	yield()
}

join_many :: proc(handles: []Handle) {
	wg := create_wait_group()
	defer destroy(wg)
	for handle in handles {
		state := get_internal_state(handle) or_continue
		assert(state.hooks[.Exit].fn == nil, "multiple join calls on the same handle")
		state.hooks[.Exit] = Closure {
			ud = transmute(rawptr)(wg),
			fn = auto_cast proc(wg: Wait_Group) {done(wg)},
		}
		add(wg)
	}
	wait(wg)
}

next_tick :: proc(fn: proc(ud: rawptr), ud: rawptr = nil) {
	queue.enqueue(&scheduler.next_tick, Closure{ud, fn})
}

timer :: proc(n: time.Duration, fn: proc(ud: rawptr), ud: rawptr = nil) -> u64 {
	res := Resource {
		id = auto_cast Internal_Resource.Timer,
		drop = proc(self: ^Resource) {},
	}
	load_inline(&res.ud, Closure)^ = Closure{ud, fn}
	id := storage.add(&scheduler.resources, res)
	tw.after(&scheduler.time_wheel, n, tw.Task(id))
	return id
}

sleep :: proc(n: time.Duration) {
	ud := get_current_internal_state()
	fn := proc(ud: rawptr) {wake(Handle(transmute(u64)(ud)))}
	timer(n, fn, transmute(rawptr)(ud.id))
	yield()
}

sleep_or_cancel :: proc(n: time.Duration, cancel: Cancel_Token) -> (ok: bool) {
	return select({branch(cancel)}, timeout = n) == -1
}

reschedule :: #force_inline proc() {
	wake(get_handle())
	yield()
}

wake :: proc(self: Handle) {
	ud, ok := get_internal_state(self)
	assert(ok, "invalid task id")
	queue.enqueue(&scheduler.ready, u64(self))
}

yield :: #force_inline proc() {
	coro.check(coro.yield(coro.running()))
}

@(private)
get_current_internal_state :: #force_inline proc() -> ^Internal_State {
	return (^Internal_State)(coro.get_user_data(coro.running()))
}

@(private)
get_internal_state :: #force_inline proc(handle: Handle) -> (state: ^Internal_State, ok: bool) {
	res := storage.get_ptr(&scheduler.resources, u64(handle)) or_return
	return transmute(^Internal_State)(res.ud[0]), true
}

get_scheduler :: #force_inline proc() -> ^Scheduler {
	return &scheduler
}

get_handle :: #force_inline proc() -> Handle {
	ud := get_current_internal_state()
	return Handle(ud.id)
}

get_pending :: #force_inline proc() -> uint {
	return scheduler.active_coroutines
}

@(private)
create_ud :: proc(fn: rawptr, args: u64 = 0) -> ^Internal_State {
	entry := storage.entry(&scheduler.resources)

	ud := new(Internal_State)
	ud.ctx = context
	ud.co = new(coro.Coro)
	ud.fn = fn
	ud.id = storage.get_id(&entry)
	ud.args = args

	res := Resource{}
	res.id = auto_cast Internal_Resource.Coroutine
	res.ud[0] = ud
	storage.insert(&entry, res)

	scheduler.active_coroutines += 1
	return ud
}

@(private)
create_desc :: proc(raw_fn: proc "c" (co: ^coro.Coro), ud: ^Internal_State) -> (desc: coro.Desc) {
	desc = coro.desc_init(raw_fn, 0)
	desc.user_data = ud
	desc.storage_size = 0
	// desc.allocator_data = ud
	// desc.alloc_cb = proc "c" (size: c.size_t, allocator_data: rawptr) -> rawptr {
	// 	ud := (^Internal_State)(allocator_data)
	// 	context = ud.ctx
	// 	ptr, _ := mem.alloc(int(size), allocator = ud.allocator)
	// 	return ptr
	// }
	// desc.dealloc_cb = proc "c" (ptr: rawptr, size: c.size_t, allocator_data: rawptr) {
	// 	ud := (^Internal_State)(allocator_data)
	// 	context = ud.ctx
	// 	mem.free_with_size(ptr, int(size), allocator = ud.allocator)
	// }
	return
}

handle_into_rawptr :: #force_inline proc(handle: Handle) -> rawptr {
	return transmute(rawptr)(handle)
}

handle_from_rawptr :: #force_inline proc(ptr: rawptr) -> Handle {
	return transmute(Handle)(ptr)
}

@(private)
call_hook :: proc(state: ^Internal_State, hook: Hook) {
	closure := state.hooks[hook]
	if closure.fn != nil do closure.fn(closure.ud)
}

load_inline :: #force_inline proc(storage: ^[$N]rawptr, $T: typeid) -> ^T {
	#assert(size_of(T) <= N * size_of(rawptr))
	#assert(align_of(T) <= align_of(rawptr))
	return (^T)(&storage[0])
}

store_inline :: #force_inline proc(storage: ^[$N]rawptr, value: $T) {
	#assert(size_of(T) <= N * size_of(rawptr))
	#assert(align_of(T) <= align_of(rawptr))
	(^T)(&storage[0])^ = value
}

try_get_resource :: #force_inline proc(id: u64) -> (^Resource, bool) {
	return storage.get_ptr(&scheduler.resources, id)
}

add_resource :: #force_inline proc(res: Resource) -> u64 {
	return storage.add(&scheduler.resources, res)
}

try_remove_resource :: #force_inline proc(id: u64) -> (Resource, bool) {
	return storage.remove(&scheduler.resources, id)
}

has_resource :: #force_inline proc(id: u64) -> bool {
	_, ok := storage.get_ptr(&scheduler.resources, id)
	return ok
}

