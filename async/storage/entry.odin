package storage

Vacant_Entry :: struct($T: typeid) {
	owner: ^Storage(T),
	idx:   u32,
	gen:   u32,
}

get_id :: proc(self: ^Vacant_Entry($T)) -> u64 {
	return pack_key(self.idx, self.gen)
}

insert :: proc(self: ^Vacant_Entry($T), value: T) -> u64 {
	slot := &self.owner.slots[self.idx]
	slot.value = value
	slot.active = true
	self.owner.count += 1
	return pack_key(self.idx, self.gen)
}

discard :: proc(self: ^Vacant_Entry) {
	slot := &self.owner.slots[self.idx]
	slot.gen += 1
	append(&self.owner.free, self.idx)
}

