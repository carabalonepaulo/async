#+build windows
package vmem

import "core:sys/windows"

@(private = "file")
page_size: uint

@(private = "file", init)
_init :: proc "contextless" () {
	info: windows.SYSTEM_INFO
	windows.GetSystemInfo(&info)
	page_size = uint(info.dwPageSize)
	windows.AddVectoredExceptionHandler(1, veh)
}

veh :: proc "system" (info: ^windows.EXCEPTION_POINTERS) -> i32 {
	record := info.ExceptionRecord

	if record.ExceptionCode != windows.EXCEPTION_ACCESS_VIOLATION {
		return windows.EXCEPTION_CONTINUE_SEARCH
	}

	block := current_block
	if block == nil {
		return windows.EXCEPTION_CONTINUE_SEARCH
	}

	fault := uintptr(record.ExceptionInformation[1])

	base := uintptr(raw_data(block))
	end := base + uintptr(len(block))

	if fault < base || fault >= end {
		return windows.EXCEPTION_CONTINUE_SEARCH
	}

	page := fault & ~(uintptr(page_size) - 1)

	committed := windows.VirtualAlloc(
		rawptr(page),
		page_size,
		windows.MEM_COMMIT,
		windows.PAGE_READWRITE,
	)

	if committed == nil {
		return windows.EXCEPTION_CONTINUE_SEARCH
	}

	return windows.EXCEPTION_CONTINUE_EXECUTION
}

_reserve :: proc "contextless" (size: int) -> ([]u8, bool) {
	data := windows.VirtualAlloc(nil, uint(size), windows.MEM_RESERVE, windows.PAGE_READWRITE)
	if data == nil do return {}, false
	return (([^]u8)(data))[:size], true
}

_release :: proc "contextless" (block: []u8) {
	_ = windows.VirtualFree(raw_data(block), 0, windows.MEM_RELEASE)
}

