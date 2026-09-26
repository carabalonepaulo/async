package vmem

import "core:mem"
import "core:testing"

Virtual_Block :: struct {
	data: rawptr,
	size: int,
}

reserve :: proc(size: int) -> ([]u8, bool) {
	return _reserve(size)
}

release :: proc(block: []u8) {
	_release(block)
}

@(test)
test :: proc(t: ^testing.T) {
	buf, ok := reserve(2 * mem.Megabyte)
	testing.expect(t, ok)
	buf[0] = 1
	buf[4096] = 1
	buf[1 * mem.Megabyte] = 1
	buf[2 * mem.Megabyte - 1] = 1
	// buf[2 * mem.Megabyte] = 1 segfault
}

