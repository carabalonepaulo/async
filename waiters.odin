package async

import "core:testing"

import il "index_list"

@(private = "file")
Inner :: struct {
	handle:   Handle,
	case_idx: int,
}

Waiter :: distinct u64

Waiters :: struct {
	list: il.Index_List(Inner),
}

@(private)
init_waiters :: proc(self: ^Waiters) {
	il.init(&self.list)
}

@(private)
deinit_waiters :: proc(self: ^Waiters) {
	for inner in il.dequeue(&self.list) {
		wake_waiter(inner.handle, inner.case_idx, false)
	}
	il.deinit(&self.list)
}

@(private)
add_waiter :: proc(self: ^Waiters, handle: Handle, case_idx: int = -1) -> Waiter {
	return Waiter(il.enqueue(&self.list, Inner{handle, case_idx}))
}

@(private)
try_remove_waiter :: proc(self: ^Waiters) -> (handle: Handle, case_idx: int, ok: bool) {
	inner := il.dequeue(&self.list) or_return
	return inner.handle, inner.case_idx, true
}

@(private)
try_remove_waiter_by_id :: proc(self: ^Waiters, id: Waiter) -> (ok: bool) {
	return il.remove(&self.list, il.Id(id))
}

@(private)
clear_waiters :: proc(self: ^Waiters) {
	il.clear(&self.list)
}

@(test)
test_try_remove_fifo_order :: proc(t: ^testing.T) {
	w: Waiters
	init_waiters(&w)
	defer deinit_waiters(&w)

	add_waiter(&w, 100, 10)
	add_waiter(&w, 200, 20)
	add_waiter(&w, 300, 30)

	h1, c1, ok1 := try_remove_waiter(&w)
	testing.expect(t, ok1)
	testing.expect_value(t, h1, 100)
	testing.expect_value(t, c1, 10)

	h2, c2, ok2 := try_remove_waiter(&w)
	testing.expect(t, ok2)
	testing.expect_value(t, h2, 200)
	testing.expect_value(t, c2, 20)

	h3, c3, ok3 := try_remove_waiter(&w)
	testing.expect(t, ok3)
	testing.expect_value(t, h3, 300)
	testing.expect_value(t, c3, 30)

	_, _, ok_empty := try_remove_waiter(&w)
	testing.expect(t, !ok_empty)
	testing.expect_value(t, w.list.used.head, -1)
	testing.expect_value(t, w.list.used.tail, -1)
}

@(test)
test_try_remove_fifo_with_reuse :: proc(t: ^testing.T) {
	w: Waiters
	init_waiters(&w)
	defer deinit_waiters(&w)

	add_waiter(&w, 100, 10)
	add_waiter(&w, 200, 20)

	h1, _, ok1 := try_remove_waiter(&w)
	testing.expect(t, ok1)
	testing.expect_value(t, h1, 100)

	add_waiter(&w, 400, 40)

	h2, _, ok2 := try_remove_waiter(&w)
	testing.expect(t, ok2)
	testing.expect_value(t, h2, 200)

	h3, _, ok3 := try_remove_waiter(&w)
	testing.expect(t, ok3)
	testing.expect_value(t, h3, 400)

	_, _, ok_end := try_remove_waiter(&w)
	testing.expect(t, !ok_end)
}

