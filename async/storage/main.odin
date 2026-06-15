package storage

INDEX_BITS :: 32
INDEX_MASK :: (1 << INDEX_BITS) - 1

Iterator :: struct($T: typeid) {
	storage: ^Storage(T),
	index:   int,
}

Slot :: struct($T: typeid) {
	gen:    u32,
	value:  T,
	active: bool,
}

Storage :: struct($T: typeid) {
	slots: [dynamic]Slot(T),
	free:  [dynamic]u32,
	count: uint,
}

init :: proc(self: ^Storage($T), capacity := 0) {
	self.slots = make([dynamic]Slot(T), 0, capacity)
	self.free = make([dynamic]u32, 0, capacity)
}

deinit :: proc(self: ^Storage($T)) {
	delete(self.slots)
	delete(self.free)
}

@(private)
reserve :: proc(self: ^Storage($T)) -> (idx: u32, gen: u32) {
	if len(self.free) > 0 {
		idx = pop(&self.free)
		gen = self.slots[idx].gen
	} else {
		idx = u32(len(self.slots))
		gen = 0

		slot := Slot(T) {
			gen    = 0,
			active = false,
		}
		append(&self.slots, slot)
	}
	return
}

entry :: proc(self: ^Storage($T)) -> Vacant_Entry(T) {
	idx, gen := reserve(self)
	return Vacant_Entry(T){self, idx, gen}
}

add :: proc(self: ^Storage($T), value: T) -> u64 {
	idx, gen := reserve(self)
	self.slots[idx].value = value
	self.slots[idx].active = true
	self.count += 1
	return pack_key(idx, gen)
}

remove :: proc(self: ^Storage($T), key: u64) -> (T, bool) {
	idx, gen := unpack_key(key)

	if idx >= u32(len(self.slots)) {
		return {}, false
	}

	slot := &self.slots[idx]
	if !slot.active || slot.gen != gen {
		return {}, false
	}

	removed_value := slot.value
	slot.value = {}
	slot.active = false
	slot.gen += 1
	self.count -= 1
	append(&self.free, idx)

	return removed_value, true
}

get_ptr :: proc(self: ^Storage($T), key: u64) -> (^T, bool) {
	idx, gen := unpack_key(key)

	if idx >= u32(len(self.slots)) {
		return nil, false
	}

	slot := &self.slots[idx]
	if !slot.active || slot.gen != gen {
		return nil, false
	}

	return &slot.value, true
}

get :: proc(self: ^Storage($T), key: u64) -> (T, bool) {
	idx, gen := unpack_key(key)

	if idx >= u32(len(self.slots)) {
		return nil, false
	}

	slot := &self.slots[idx]
	if !slot.active || slot.gen != gen {
		return nil, false
	}

	return slot.value, true
}

count :: #force_inline proc(self: ^Storage($T)) -> uint {
	return self.count
}

retain :: proc(self: ^Storage($T), ud: rawptr, f: proc(key: u64, val: ^T, ud: rawptr) -> bool) {
	for i := 0; i < len(self.slots); i += 1 {
		slot := &self.slots[i]
		if slot.active {
			key := pack_key(u32(i), slot.gen)
			if !f(key, &slot.value, ud) {
				slot.value = {}
				slot.active = false
				slot.gen += 1
				self.count -= 1
				append(&self.free, u32(i))
			}
		}
	}
}

iter :: proc(self: ^Storage($T)) -> Iterator(T) {
	return Iterator(T){storage = self, index = 0}
}

iterate :: proc(it: ^Iterator($T)) -> (key: u64, val: ^T, ok: bool) {
	s := it.storage
	for it.index < len(s.slots) {
		idx := it.index
		it.index += 1

		slot := &s.slots[idx]
		if slot.active {
			k := pack_key(u32(idx), slot.gen)
			return k, &slot.value, true
		}
	}
	return 0, nil, false
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

