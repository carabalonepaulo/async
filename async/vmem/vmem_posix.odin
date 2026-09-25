#+build darwin, netbsd, freebsd, openbsd
package vmem

import "core:sys/posix"

_reserve :: proc "contextless" (size: int) -> ([]u8, bool) {
	prot: posix.Prot_Flags

	when ODIN_OS == .Darwin || ODIN_OS == .OpenBSD {
		prot = {.READ, .WRITE}
	}

	when ODIN_OS == .FreeBSD {
		PROT_MAX :: proc "contextless" (flags: posix.Prot_Flags) -> posix.Prot_Flags {
			_PROT_MAX_SHIFT :: 16
			return transmute(posix.Prot_Flags)(transmute(i32)flags << _PROT_MAX_SHIFT)
		}
		prot = PROT_MAX({.READ, .WRITE})
	}

	when ODIN_OS == .NetBSD {
		PROT_MPROTECT :: proc "contextless" (flags: posix.Prot_Flags) -> posix.Prot_Flags {
			return transmute(posix.Prot_Flags)(transmute(i32)flags << 3)
		}
		prot = PROT_MPROTECT({.READ, .WRITE})
	}

	addr := posix.mmap(nil, size, prot, {.ANONYMOUS, .PRIVATE})
	if addr == posix.MAP_FAILED do return {}, false
	return (([^]u8)(addr))[:size], true
}

_release :: proc "contextless" (block: []u8) {
	posix.munmap(raw_data(block), len(block))
}

