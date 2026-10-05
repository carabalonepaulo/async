package async_http_server_headers

import "core:strings"

Add_Mode :: enum {
	If_Absent,
	Append,
	Replace,
}

Header :: struct {
	key:   string,
	value: string,
}

Headers :: distinct [dynamic]Header

destroy :: proc(self: ^Headers) {
	for header in self {
		delete(header.key)
		delete(header.value)
	}
	delete(self^)
}

get :: proc(self: ^Headers, key: string) -> (value: string, ok: bool) {
	idx := find(self, key)
	if idx > -1 do return self[idx].value, true
	return "", false
}

set :: proc(self: ^Headers, key: string, value: string) {
	idx := find(self, key)
	if idx > -1 do self[idx].value = strings.clone(value)
	else do append(self, Header{strings.clone(key), strings.clone(value)})
}

remove :: proc(self: ^Headers, key: string) {
	#reverse for &header, i in self {
		if strings.equal_fold(header.key, key) {
			delete(header.key)
			delete(header.value)
			unordered_remove(self, i)
		}
	}
}

has :: proc(self: ^Headers, key: string) -> bool {
	return find(self, key) > -1
}

add :: proc(self: ^Headers, key: string, value: string, mode := Add_Mode.Append) {
	switch mode {
	case .If_Absent:
		if !has(self, key) do append(self, Header{strings.clone(key), strings.clone(value)})
	case .Replace:
		idx := find(self, key)
		if idx > -1 {
			delete(self[idx].value)
			self[idx].value = strings.clone(value)
		} else do append(self, Header{strings.clone(key), strings.clone(value)})
	case .Append:
		append(self, Header{strings.clone(key), strings.clone(value)})
	}
}

Iter :: struct {
	headers: ^Headers,
	idx:     int,
}

iter :: proc(self: ^Headers) -> Iter {
	return Iter{headers = self, idx = 0}
}

iterate :: proc(self: ^Iter, filter: string = "") -> (header: Header, idx: int, ok: bool) {
	for self.idx < len(self.headers) {
		current := self.headers[self.idx]
		idx := self.idx
		self.idx += 1

		if len(filter) == 0 || strings.equal_fold(current.key, filter) {
			return current, idx, true
		}
	}
	return {}, -1, false
}

find_token :: proc(self: ^Headers, key: string, token: string) -> int {
	it := iter(self)
	for header, idx in iterate(&it, key) {
		val_it := header.value
		for value in strings.split_iterator(&val_it, ",") {
			if strings.equal_fold(strings.trim_space(value), token) {
				return idx
			}
		}
	}
	return -1
}

has_token :: #force_inline proc(self: ^Headers, key: string, token: string) -> bool {
	return find_token(self, key, token) != -1
}

@(private)
find :: proc(self: ^Headers, key: string) -> int {
	for i in 0 ..< len(self) {
		if strings.equal_fold(self[i].key, key) {
			return i
		}
	}
	return -1
}

