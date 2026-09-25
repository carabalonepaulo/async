package vmem

import "core:mem"
import "core:testing"

Virtual_Block :: struct {
	data: rawptr,
	size: int,
}

@(thread_local)
current_block: []u8

@(deferred_out = GUARD_END)
guard :: proc() -> []u8 {
	return current_block
}

@(private = "file")
GUARD_END :: proc(buf: []u8) {
	current_block = buf
}

reserve :: proc "contextless" (size: int) -> ([]u8, bool) {
	return _reserve(size)
}

release :: proc "contextless" (block: []u8) {
	_release(block)
}

@(test)
test :: proc(t: ^testing.T) {
	buf, ok := reserve(2 * mem.Megabyte)
	testing.expect(t, ok)

	current_block = buf
	defer current_block = nil

	buf[1 * mem.Megabyte] = 1
}

