package async

import "base:builtin"
import "core:testing"

INDEX_BITS :: 32
INDEX_MASK :: (1 << INDEX_BITS) - 1

@(private = "file")
Node :: struct {
	next: int,
	prev: int,
}

@(private = "file")
List :: struct {
	head: int,
	tail: int,
}

@(private = "file")
Slot :: struct {
	handle:   Handle,
	case_idx: int,
	node:     Node,
	gen:      u32,
}

Waiter :: distinct u64

Waiters :: struct {
	waiters: [dynamic]Slot,
	free:    List,
	used:    List,
}

init_waiters :: proc(self: ^Waiters) {
	self.waiters = make([dynamic]Slot)
	self.used = List{-1, -1}
	self.free = List{-1, -1}
}

deinit_waiters :: proc(self: ^Waiters) {
	for handle, case_idx in try_remove_waiter(self) do wake_waiter(Handle(handle), case_idx, false)
	builtin.delete(self.waiters)
	self.used = List{-1, -1}
	self.free = List{-1, -1}
}

add_waiter :: proc(self: ^Waiters, handle: Handle, case_idx: int = -1) -> Waiter {
	idx, waiter := find_empty(self)
	waiter.handle = handle
	waiter.case_idx = case_idx
	link(&self.used, idx, &waiter.node, self.waiters[:])
	return Waiter(pack_key(u32(idx), waiter.gen))
}

try_remove_waiter :: proc(self: ^Waiters) -> (handle: Handle, case_idx: int, ok: bool) {
	if self.used.head == -1 do return 0, 0, false
	#no_bounds_check {
		idx := self.used.head
		waiter := &self.waiters[idx]
		handle = waiter.handle
		case_idx = waiter.case_idx
		ok = true

		waiter.handle = 0
		waiter.gen += 1
		unlink(&self.used, idx, &waiter.node, self.waiters[:])
		link(&self.free, idx, &waiter.node, self.waiters[:])
		return
	}
}

try_remove_waiter_by_id :: proc(self: ^Waiters, id: Waiter) -> (ok: bool) {
	#no_bounds_check {
		idx, waiter := get_ptr(self, id) or_return
		waiter.handle = 0
		waiter.gen += 1
		unlink(&self.used, idx, &waiter.node, self.waiters[:])
		link(&self.free, idx, &waiter.node, self.waiters[:])
	}
	return true
}

@(private = "file")
get_ptr :: proc(self: ^Waiters, id: Waiter) -> (int, ^Slot, bool) {
	idx, gen := unpack_key(u64(id))
	if int(idx) >= builtin.len(self.waiters) do return 0, nil, false

	waiter := &self.waiters[idx]
	if waiter.handle != 0 && waiter.gen == gen {
		return int(idx), waiter, true
	}
	return 0, nil, false
}

waiters_clear :: proc(self: ^Waiters) {
	builtin.clear(&self.waiters)
	self.used = List{-1, -1}
	self.free = List{-1, -1}
}

// Iter :: struct {
// 	self: ^Waiters,
// 	idx:  int,
// }

// waiters_iter :: proc(self: ^Waiters) -> Iter {
// 	return Iter{self, self.used.head}
// }

// iterate_waiters :: proc(it: ^Iter) -> (handle: u64, case_idx: int, ok: bool) {
// 	curr := it.idx
// 	if curr == -1 do return 0, 0, false

// 	waiter := &it.self.waiters[curr]
// 	it.idx = waiter.node.next
// 	return waiter.handle, waiter.case_idx, true
// }

@(private = "file")
find_empty :: proc(self: ^Waiters) -> (int, ^Slot) {
	if self.free.head == -1 {
		idx := builtin.len(self.waiters)
		append(&self.waiters, Slot{node = Node{-1, -1}, gen = 1})
		return idx, &self.waiters[idx]
	} else {
		idx := self.free.tail
		waiter := &self.waiters[idx]
		unlink(&self.free, idx, &waiter.node, self.waiters[:])
		return idx, waiter
	}
}

@(private)
link :: proc(list: ^List, idx: int, node: ^Node, container: []Slot) {
	if list.head == -1 {
		list.head = idx
		list.tail = idx
	} else {
		prev_tail := &container[list.tail]
		prev_tail.node.next = idx

		node.prev = list.tail
		list.tail = idx
	}
}

@(private)
unlink :: proc(list: ^List, idx: int, node: ^Node, container: []Slot) {
	if node.prev != -1 do container[node.prev].node.next = node.next
	else do list.head = node.next

	if node.next != -1 do container[node.next].node.prev = node.prev
	else do list.tail = node.prev

	node.prev = -1
	node.next = -1
}

@(private)
pack_key :: proc(idx: u32, gen: u32) -> u64 {
	return ((u64(gen)) << INDEX_BITS) | (u64(idx) & INDEX_MASK)
}

@(private)
unpack_key :: proc(key: u64) -> (u32, u32) {
	idx := u32(key & INDEX_MASK)
	gen := u32(key >> INDEX_BITS)
	return idx, gen
}

@(test)
test_init_and_deinit :: proc(t: ^testing.T) {
	w: Waiters
	init_waiters(&w)
	defer deinit_waiters(&w)

	testing.expect_value(t, builtin.len(w.waiters), 0)
	testing.expect_value(t, w.used.head, -1)
	testing.expect_value(t, w.used.tail, -1)
	testing.expect_value(t, w.free.head, -1)
	testing.expect_value(t, w.free.tail, -1)
}

@(test)
test_add_and_find :: proc(t: ^testing.T) {
	w: Waiters
	init_waiters(&w)
	defer deinit_waiters(&w)

	id1 := add_waiter(&w, 100, 1)
	id2 := add_waiter(&w, 200, 2)
	id3 := add_waiter(&w, 300, 3)

	testing.expect_value(t, builtin.len(w.waiters), 3)

	idx, waiter, ok := get_ptr(&w, id2)
	testing.expect(t, ok)
	testing.expect_value(t, waiter.case_idx, 2)
	testing.expect_value(t, idx, 1)

	_, _, found_invalid := get_ptr(&w, 999)
	testing.expect(t, !found_invalid)
}

@(test)
test_try_remove :: proc(t: ^testing.T) {
	w: Waiters
	init_waiters(&w)
	defer deinit_waiters(&w)

	add_waiter(&w, 100, 1)
	id2 := add_waiter(&w, 200, 2)
	add_waiter(&w, 300, 3)

	ok := try_remove_waiter_by_id(&w, id2)
	testing.expect(t, ok)

	_, _, found := get_ptr(&w, 200)
	testing.expect(t, !found)

	testing.expect_value(t, w.free.head, 1)
	testing.expect_value(t, w.free.tail, 1)

	ok_again := try_remove_waiter_by_id(&w, id2)
	testing.expect(t, !ok_again)
}

@(test)
test_remove_head_and_tail :: proc(t: ^testing.T) {
	w: Waiters
	init_waiters(&w)
	defer deinit_waiters(&w)

	id1 := add_waiter(&w, 100, 1)
	id2 := add_waiter(&w, 200, 2)

	try_remove_waiter_by_id(&w, id1)
	testing.expect_value(t, w.used.head, 1)
	testing.expect_value(t, w.used.tail, 1)

	try_remove_waiter_by_id(&w, id2)
	testing.expect_value(t, w.used.head, -1)
	testing.expect_value(t, w.used.tail, -1)
}

@(test)
test_reuse_free_nodes :: proc(t: ^testing.T) {
	w: Waiters
	init_waiters(&w)
	defer deinit_waiters(&w)

	add_waiter(&w, 100, 1)
	id2 := add_waiter(&w, 200, 2)
	try_remove_waiter_by_id(&w, id2)
	id4 := add_waiter(&w, 400, 4)

	testing.expect_value(t, builtin.len(w.waiters), 2)

	idx, waiter, found := get_ptr(&w, id4)
	testing.expect(t, found)
	testing.expect_value(t, idx, 1)
	testing.expect_value(t, waiter.case_idx, 4)

	testing.expect_value(t, w.free.head, -1)
	testing.expect_value(t, w.free.tail, -1)
}

@(test)
test_generational_id_invalidation :: proc(t: ^testing.T) {
	w: Waiters
	init_waiters(&w)
	defer deinit_waiters(&w)

	old_id := add_waiter(&w, 100, 1)

	ok_rem := try_remove_waiter_by_id(&w, old_id)
	testing.expect(t, ok_rem)

	new_id := add_waiter(&w, 200, 2)

	old_idx, old_gen := unpack_key(u64(old_id))
	new_idx, new_gen := unpack_key(u64(new_id))

	testing.expect_value(t, old_idx, new_idx)
	testing.expect_value(t, new_gen, old_gen + 1)
	testing.expect(t, old_id != new_id)

	_, _, found_old := get_ptr(&w, old_id)
	testing.expect(t, !found_old)

	ok_rem_old := try_remove_waiter_by_id(&w, old_id)
	testing.expect(t, !ok_rem_old)

	_, waiter, found_new := get_ptr(&w, new_id)
	testing.expect(t, found_new)
	testing.expect_value(t, waiter.handle, 200)
}

@(test)
test_clear :: proc(t: ^testing.T) {
	w: Waiters
	init_waiters(&w)
	defer deinit_waiters(&w)

	add_waiter(&w, 100, 1)
	add_waiter(&w, 200, 2)

	waiters_clear(&w)

	testing.expect_value(t, builtin.len(w.waiters), 0)
	testing.expect_value(t, w.used.head, -1)
	testing.expect_value(t, w.used.tail, -1)
	testing.expect_value(t, w.free.head, -1)
	testing.expect_value(t, w.free.tail, -1)
}

// @(test)
// test_iter_empty :: proc(t: ^testing.T) {
// 	w: Waiters
// 	init_waiters(&w)
// 	defer deinit_waiters(&w)

// 	it := waiters_iter(&w)
// 	_, _, ok := iterate_waiters(&it)
// 	testing.expect(t, !ok)
// }

// @(test)
// test_iter_all_elements :: proc(t: ^testing.T) {
// 	w: Waiters
// 	init_waiters(&w)
// 	defer deinit_waiters(&w)

// 	add_waiter(&w, 100, 10)
// 	add_waiter(&w, 200, 20)
// 	add_waiter(&w, 300, 30)

// 	it := waiters_iter(&w)

// 	h1, c1, ok1 := iterate_waiters(&it)
// 	testing.expect(t, ok1)
// 	testing.expect_value(t, h1, 100)
// 	testing.expect_value(t, c1, 10)

// 	h2, c2, ok2 := iterate_waiters(&it)
// 	testing.expect(t, ok2)
// 	testing.expect_value(t, h2, 200)
// 	testing.expect_value(t, c2, 20)

// 	h3, c3, ok3 := iterate_waiters(&it)
// 	testing.expect(t, ok3)
// 	testing.expect_value(t, h3, 300)
// 	testing.expect_value(t, c3, 30)

// 	_, _, ok4 := iterate_waiters(&it)
// 	testing.expect(t, !ok4)
// }

// @(test)
// test_iter_after_removals_and_repositioning :: proc(t: ^testing.T) {
// 	w: Waiters
// 	init_waiters(&w)
// 	defer deinit_waiters(&w)

// 	add_waiter(&w, 100, 10)
// 	id2 := add_waiter(&w, 200, 20)
// 	add_waiter(&w, 300, 30)

// 	try_remove_waiter_by_id(&w, id2)

// 	add_waiter(&w, 400, 40)

// 	it := waiters_iter(&w)

// 	h1, _, _ := iterate_waiters(&it)
// 	testing.expect_value(t, h1, 100)

// 	h2, _, _ := iterate_waiters(&it)
// 	testing.expect_value(t, h2, 300)

// 	h3, _, _ := iterate_waiters(&it)
// 	testing.expect_value(t, h3, 400)

// 	_, _, ok_end := iterate_waiters(&it)
// 	testing.expect(t, !ok_end)
// }

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
	testing.expect_value(t, w.used.head, -1)
	testing.expect_value(t, w.used.tail, -1)
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

