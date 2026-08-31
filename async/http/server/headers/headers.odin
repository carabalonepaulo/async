package async_http_server_headers

import "core:strings"

Header :: struct {
	key:   string,
	value: string,
}

Headers :: distinct [dynamic]Header

get :: proc(self: ^Headers, key: string) -> (value: string, ok: bool) {
	for &header in self {
		if strings.equal_fold(header.key, key) {
			return header.value, true
		}
	}
	return "", false
}

set :: proc(self: ^Headers, key: string, value: string) {
	for &header in self {
		if strings.equal_fold(header.key, key) {
			header.value = value
			return
		}
	}
	append(self, Header{key, value})
}

delete :: proc(self: ^Headers, key: string) {
	for i in 0 ..< len(self) {
		if strings.equal_fold(self[i].key, key) {
			unordered_remove(self, i)
			return
		}
	}
}

add :: proc(self: ^Headers, key: string, value: string) {
	append(self, Header{key, value})
}

