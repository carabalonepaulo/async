package async

import "core:container/queue"

schedule :: proc {
	schedule_without_data,
	schedule_with_poly,
	schedule_with_poly2,
	schedule_with_poly3,
}

schedule_without_data :: proc(fn: proc() -> bool) {
	task: Task
	task.ud[0] = transmute(rawptr)(fn)
	task.fn = proc(ud: ^[CASE_INLINE_STORAGE]rawptr) -> bool {
		fn := transmute(proc() -> bool)(ud[0])
		return fn()
	}
	queue.enqueue(&scheduler.scheduled, task)
}

schedule_with_poly :: proc(a: $A, fn: proc(a: A) -> bool) {
	State :: struct {
		fn: proc(a: A) -> bool,
		a:  A,
	}

	task: Task
	load_inline(&task.ud, State)^ = State{fn, a}

	task.fn = proc(ud: ^[CASE_INLINE_STORAGE]rawptr) -> bool {
		state := load_inline(ud, State)
		return state.fn(state.a)
	}
}

schedule_with_poly2 :: proc(a: $A, b: $B, fn: proc(a: A, b: B) -> bool) {
	State :: struct {
		fn: proc(a: A, b: B) -> bool,
		a:  A,
		b:  B,
	}

	task: Task
	load_inline(&task.ud, State)^ = State{fn, a, b}

	task.fn = proc(ud: ^[CASE_INLINE_STORAGE]rawptr) -> bool {
		state := load_inline(ud, State)
		return state.fn(state.a, state.b)
	}
}

schedule_with_poly3 :: proc(a: $A, b: $B, c: $C, fn: proc(a: A, b: B, c: C) -> bool) {
	State :: struct {
		fn: proc(a: A, b: B, c: C) -> bool,
		a:  A,
		b:  B,
		c:  C,
	}

	task: Task
	load_inline(&task.ud, State)^ = State{fn, a, b, c}

	task.fn = proc(ud: ^[CASE_INLINE_STORAGE]rawptr) -> bool {
		state := load_inline(ud, State)
		return state.fn(state.a, state.b, state.c)
	}
}
