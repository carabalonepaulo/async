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

@(private)
find :: proc(self: ^Headers, key: string) -> int {
	for i in 0 ..< len(self) {
		if strings.equal_fold(self[i].key, key) {
			return i
		}
	}
	return -1
}

