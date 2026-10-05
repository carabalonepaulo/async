package async_http_server_headers

import "core:fmt"
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

get :: proc(self: ^Headers, key: string) -> (value: string, ok: bool) {
	idx := find(self, key)
	if idx > -1 do return self[idx].value, true
	return "", false
}

set :: proc(self: ^Headers, key: string, value: string) {
	idx := find(self, key)
	if idx > -1 do self[idx].value = value
	else do append(self, Header{key, value})
}

delete :: proc(self: ^Headers, key: string) {
	#reverse for &header, i in self {
		if strings.equal_fold(header.key, key) {
			unordered_remove(self, i)
			fmt.println("delete", i, key, header.value)
		}
	}
}

has :: proc(self: ^Headers, key: string) -> bool {
	return find(self, key) > -1
}

add :: proc(self: ^Headers, key: string, value: string, mode := Add_Mode.Append) {
	switch mode {
	case .If_Absent:
		if !has(self, key) do append(self, Header{key, value})
	case .Replace:
		idx := find(self, key)
		if idx > -1 do self[idx].value = value
		else do append(self, Header{key, value})
	case .Append:
		append(self, Header{key, value})
	}
}

Iter :: struct {
	headers: ^Headers,
	idx:     int,
}

iter :: proc(self: ^Headers) -> Iter {
	return Iter{headers = self, idx = 0}
}

iterate :: proc(self: ^Iter, filter: string = "") -> (header: ^Header, idx: int, ok: bool) {
	for self.idx < len(self.headers) {
		current := &self.headers[self.idx]
		idx := self.idx
		self.idx += 1

		if len(filter) == 0 || strings.equal_fold(current.key, filter) {
			return current, idx, true
		}
	}
	return nil, -1, false
}

find_token :: proc(self: ^Headers, key: string, token: string) -> int {
	it := iter(self)
	for header, idx in iterate(&it, key) {
		for value in strings.split_iterator(&header.value, ",") {
			if strings.equal_fold(value, token) {
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

