package async

import "base:builtin"
import "core:time"

import "coro"
import "storage"

@(private)
Case :: struct {
	ud:          [MAX_USER_DATA]rawptr,
	is_alive:    proc(self: ^Case) -> bool,
	try:         proc(self: ^Case) -> bool,
	complete:    proc(self: ^Case, ok: bool),
	subscribe:   proc(self: ^Case, handle: Handle, case_idx: int),
	unsubscribe: proc(self: ^Case, handle: Handle),
}

select :: any

any :: proc(cases: []Case, timeout: time.Duration = -1) -> int {
	sched := get_scheduler()

	for &c, i in cases {
		if !c.is_alive(&c) {
			c.complete(&c, false)
			return i
		}
		if c.try(&c) do return i
	}

	if timeout == 0 do return -1

	handle := get_handle()
	for &c, i in cases do c.subscribe(&c, handle, i)

	timer_id: u64
	if timeout > 0 {
		fn := proc(ud: rawptr) {wake(Handle(transmute(u64)(ud)))}
		timer_id = timer(timeout, fn, transmute(rawptr)(handle))
	}

	yield()

	ud := get_internal_state()
	idx: int

	if coro.get_bytes_stored(ud.co) >= size_of(int) {
		raw_idx := pop(int)
		storage.remove(&sched.timers, timer_id)

		if raw_idx < 0 {
			idx = (-raw_idx) - 1
			for &c, i in cases {
				if i != idx && c.is_alive(&c) {
					c.unsubscribe(&c, handle)
				}
			}
			return idx
		} else do idx = raw_idx
		cases[idx].complete(&cases[idx], raw_idx >= 0)
	} else {
		idx = -1
	}

	for &c in cases do if c.is_alive(&c) do c.unsubscribe(&c, handle)
	return idx
}

all :: proc(cases: []Case, timeout: time.Duration = -1) -> int {
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
wake_case :: proc(handle: Handle, case_idx: int) {
	sched := get_scheduler()
	state, ok := storage.get(&sched.slots, u64(handle))
	if !ok do return
	if coro.get_bytes_stored(state.co) > 0 do return
	send(handle, -(case_idx + 1))
}

