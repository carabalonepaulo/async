package async_index_list

import "base:builtin"
import "core:testing"

@(private = "file")
INDEX_BITS :: 32

@(private = "file")
INDEX_MASK :: (1 << INDEX_BITS) - 1

@(private = "file")
Item :: struct($T: typeid) {
	value: T,
	gen:   u32,
	next:  int,
	prev:  int,
}

@(private = "file")
List :: struct {
	head: int,
	tail: int,
}

Index_List :: struct($T: typeid) {
	items: [dynamic]Item(T),
	used:  List,
	free:  List,
}

Id :: distinct u64

init :: proc(self: ^Index_List($T)) {
	self.items = make([dynamic]Item(T))
	self.used = List{-1, -1}
	self.free = List{-1, -1}
}

deinit :: proc(self: ^Index_List($T)) {
	builtin.delete(self.items)
	self.used = List{-1, -1}
	self.free = List{-1, -1}
}

enqueue :: add

dequeue :: proc(self: ^Index_List($T)) -> (value: T, ok: bool) {
	if self.used.head == -1 do return {}, false
	#no_bounds_check {
		idx := self.used.head
		item := &self.items[idx]
		value = item.value
		ok = true

		item.value = {}
		item.gen += 1
		unlink(&self.used, idx, item, self.items[:])
		link(&self.free, idx, item, self.items[:])
		return
	}
}

add :: proc(self: ^Index_List($T), value: T) -> Id {
	idx, item := find_empty(self)
	item.value = value
	link(&self.used, idx, item, self.items[:])
	return Id(pack_key(u32(idx), item.gen))
}

remove :: proc(self: ^Index_List($T), id: Id) -> (ok: bool) {
	#no_bounds_check {
		idx, item := get_ptr(self, id) or_return
		item.value = {}
		item.gen += 1
		unlink(&self.used, idx, item, self.items[:])
		link(&self.free, idx, item, self.items[:])
	}
	return true
}

Iter :: struct($T: typeid) {
	self: ^Index_List(T),
	idx:  int,
}

iter :: proc(self: ^Index_List($T)) -> Iter(T) {
	return Iter(T){self, self.used.head}
}

iterate :: proc(it: ^Iter($T)) -> (id: Id, value: ^T, ok: bool) {
	curr := it.idx
	if curr == -1 do return 0, nil, false

	#no_bounds_check {
		item := &it.self.items[curr]
		id = Id(pack_key(u32(it.idx), item.gen))
		value = &item.value
		ok = true

		it.idx = item.next
	}

	return
}

clear :: proc(self: ^Index_List($T)) {
	builtin.clear(&self.items)
	self.used = List{-1, -1}
	self.free = List{-1, -1}
}

@(private = "file")
get_ptr :: proc(self: ^Index_List($T), id: Id) -> (int, ^Item(T), bool) {
	idx, gen := unpack_key(u64(id))
	if int(idx) >= builtin.len(self.items) do return 0, nil, false

	#no_bounds_check {
		item := &self.items[idx]
		if item.gen == gen do return int(idx), item, true
		return 0, nil, false
	}
}

@(private = "file")
find_empty :: proc(self: ^Index_List($T)) -> (int, ^Item(T)) {
	#no_bounds_check {
		if self.free.head == -1 {
			idx := builtin.len(self.items)
			append(&self.items, Item(T){gen = 1, next = -1, prev = -1})
			return idx, &self.items[idx]
		} else {
			idx := self.free.tail
			item := &self.items[idx]
			unlink(&self.free, idx, item, self.items[:])
			return idx, item
		}
	}
}

@(private = "file")
link :: proc(list: ^List, idx: int, node: ^Item($T), container: []Item(T)) {
	#no_bounds_check {
		if list.head == -1 {
			list.head = idx
			list.tail = idx
		} else {
			prev_tail := &container[list.tail]
			prev_tail.next = idx

			node.prev = list.tail
			list.tail = idx
		}
	}
}

@(private = "file")
unlink :: proc(list: ^List, idx: int, node: ^Item($T), container: []Item(T)) {
	#no_bounds_check {
		if node.prev != -1 do container[node.prev].next = node.next
		else do list.head = node.next

		if node.next != -1 do container[node.next].prev = node.prev
		else do list.tail = node.prev

		node.prev = -1
		node.next = -1
	}
}

@(private = "file")
pack_key :: proc(idx: u32, gen: u32) -> u64 {
	return ((u64(gen)) << INDEX_BITS) | (u64(idx) & INDEX_MASK)
}

@(private = "file")
unpack_key :: proc(key: u64) -> (u32, u32) {
	idx := u32(key & INDEX_MASK)
	gen := u32(key >> INDEX_BITS)
	return idx, gen
}

@(test)
test_init_and_deinit :: proc(t: ^testing.T) {
	w: Index_List(int)
	init(&w)
	defer deinit(&w)

	testing.expect_value(t, builtin.len(w.items), 0)
	testing.expect_value(t, w.used.head, -1)
	testing.expect_value(t, w.used.tail, -1)
	testing.expect_value(t, w.free.head, -1)
	testing.expect_value(t, w.free.tail, -1)
}

@(test)
test_add_and_find :: proc(t: ^testing.T) {
	w: Index_List(int)
	init(&w)
	defer deinit(&w)

	id1 := add(&w, 100)
	id2 := add(&w, 200)
	id3 := add(&w, 300)

	testing.expect_value(t, builtin.len(w.items), 3)

	idx, item, ok := get_ptr(&w, id2)
	testing.expect(t, ok)
	testing.expect_value(t, item.value, 200)
	testing.expect_value(t, idx, 1)

	_, _, found_invalid := get_ptr(&w, 999)
	testing.expect(t, !found_invalid)
}

@(test)
test_try_remove :: proc(t: ^testing.T) {
	w: Index_List(int)
	init(&w)
	defer deinit(&w)

	add(&w, 100)
	id2 := add(&w, 200)
	add(&w, 300)

	ok := remove(&w, id2)
	testing.expect(t, ok)

	_, _, found := get_ptr(&w, 200)
	testing.expect(t, !found)

	testing.expect_value(t, w.free.head, 1)
	testing.expect_value(t, w.free.tail, 1)

	ok_again := remove(&w, id2)
	testing.expect(t, !ok_again)
}

@(test)
test_remove_head_and_tail :: proc(t: ^testing.T) {
	w: Index_List(int)
	init(&w)
	defer deinit(&w)

	id1 := add(&w, 100)
	id2 := add(&w, 200)

	remove(&w, id1)
	testing.expect_value(t, w.used.head, 1)
	testing.expect_value(t, w.used.tail, 1)

	remove(&w, id2)
	testing.expect_value(t, w.used.head, -1)
	testing.expect_value(t, w.used.tail, -1)
}

@(test)
test_reuse_free_nodes :: proc(t: ^testing.T) {
	w: Index_List(int)
	init(&w)
	defer deinit(&w)

	add(&w, 100)
	id2 := add(&w, 200)
	remove(&w, id2)
	id4 := add(&w, 400)

	testing.expect_value(t, builtin.len(w.items), 2)

	idx, item, found := get_ptr(&w, id4)
	testing.expect(t, found)
	testing.expect_value(t, idx, 1)
	testing.expect_value(t, item.value, 400)

	testing.expect_value(t, w.free.head, -1)
	testing.expect_value(t, w.free.tail, -1)
}

@(test)
test_generational_id_invalidation :: proc(t: ^testing.T) {
	w: Index_List(int)
	init(&w)
	defer deinit(&w)

	old_id := add(&w, 100)

	ok_rem := remove(&w, old_id)
	testing.expect(t, ok_rem)

	new_id := add(&w, 200)

	old_idx, old_gen := unpack_key(u64(old_id))
	new_idx, new_gen := unpack_key(u64(new_id))

	testing.expect_value(t, old_idx, new_idx)
	testing.expect_value(t, new_gen, old_gen + 1)
	testing.expect(t, old_id != new_id)

	_, _, found_old := get_ptr(&w, old_id)
	testing.expect(t, !found_old)

	ok_rem_old := remove(&w, old_id)
	testing.expect(t, !ok_rem_old)

	_, item, found_new := get_ptr(&w, new_id)
	testing.expect(t, found_new)
	testing.expect_value(t, item.value, 200)
}

@(test)
test_clear :: proc(t: ^testing.T) {
	w: Index_List(int)
	init(&w)
	defer deinit(&w)

	add(&w, 100)
	add(&w, 200)

	clear(&w)

	testing.expect_value(t, builtin.len(w.items), 0)
	testing.expect_value(t, w.used.head, -1)
	testing.expect_value(t, w.used.tail, -1)
	testing.expect_value(t, w.free.head, -1)
	testing.expect_value(t, w.free.tail, -1)
}

@(test)
test_iter_empty :: proc(t: ^testing.T) {
	w: Index_List(int)
	init(&w)
	defer deinit(&w)

	it := iter(&w)
	_, _, ok := iterate(&it)
	testing.expect(t, !ok)
}

@(test)
test_iter_all_elements :: proc(t: ^testing.T) {
	w: Index_List(int)
	init(&w)
	defer deinit(&w)

	add(&w, 100)
	add(&w, 200)
	add(&w, 300)

	it := iter(&w)

	h1, c1, ok1 := iterate(&it)
	testing.expect(t, ok1)
	testing.expect_value(t, c1^, 100)

	h2, c2, ok2 := iterate(&it)
	testing.expect(t, ok2)
	testing.expect_value(t, c2^, 200)

	h3, c3, ok3 := iterate(&it)
	testing.expect(t, ok3)
	testing.expect_value(t, c3^, 300)

	_, _, ok4 := iterate(&it)
	testing.expect(t, !ok4)
}

@(test)
test_iterate_and_remove :: proc(t: ^testing.T) {
	list: Index_List(int)
	init(&list)
	defer deinit(&list)

	for i in 0 ..< 5 {
		add(&list, i)
	}

	it := iter(&list)

	count := 0
	for {
		id, value, ok := iterate(&it)
		if !ok do break

		testing.expect_value(t, value^, count)
		testing.expect_value(t, remove(&list, id), true)

		count += 1
	}

	testing.expect_value(t, count, 5)
	testing.expect_value(t, list.used.head, -1)
	testing.expect_value(t, list.used.tail, -1)
}

