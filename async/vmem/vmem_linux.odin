#+build linux
package vmem

import "core:sys/linux"

_reserve :: proc "contextless" (size: int) -> ([]u8, bool) {
	addr, errno := linux.mmap(nil, size, {.READ, .WRITE}, {.PRIVATE, .ANONYMOUS})
	if errno == .ENOMEM || errno == .EINVAL do return nil, false
	return (([^]u8)(addr))[:size], true
}

_release :: proc "contextless" (block: []u8) {
	_ = linux.munmap(raw_data(block), len(block))
}

