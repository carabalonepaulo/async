package async

import "base:builtin"
import "core:time"

import "storage"

@(private)
Case :: struct {
	ud:          [CASE_INLINE_STORAGE]rawptr,
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

	idx: int
	ok: bool

	if winner, ok := take_winner(); ok {
		if timeout > 0 do storage.remove(&sched.resources, timer_id)

		idx, ok = decode_idx(winner)
		if !ok {
			for &c, i in cases {
				if i != idx && c.is_alive(&c) {
					c.unsubscribe(&c, handle)
				}
			}
			return idx
		}
		cases[idx].complete(&cases[idx], ok)
	} else do idx = -1

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
take_winner :: proc() -> (value: int, ok: bool) {
	state := get_current_internal_state()
	value, ok = state.winner.(int)
	state.winner = nil
	return
}

@(private)
wake_case :: proc(handle: Handle, case_idx: int, ok: bool, loc := #caller_location) -> bool {
	state := get_internal_state(handle) or_return
	state.winner = encode_idx(case_idx, ok)
	wake(handle)
	return true
}

@(private)
encode_idx :: #force_inline proc(case_idx: int, ok: bool) -> (encoded: int) {
	encoded = case_idx + 1
	if !ok do encoded = -encoded
	return
}

@(private)
decode_idx :: #force_inline proc(raw_idx: int) -> (decoded: int, ok: bool) {
	ok = raw_idx > 0
	decoded = raw_idx < 0 ? (-raw_idx) - 1 : raw_idx - 1
	return
}

