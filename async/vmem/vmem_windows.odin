#+build windows
package vmem

import "base:runtime"
import "core:container/rbtree"
import "core:sys/windows"

@(private = "file")
Block_Tree :: rbtree.Tree(uintptr, uintptr)

@(private = "file", thread_local)
blocks: Block_Tree

@(private = "file", thread_local)
blocks_initialized: bool

@(private = "file")
page_size: uint

@(private = "file", init)
_init :: proc "contextless" () {
	info: windows.SYSTEM_INFO
	windows.GetSystemInfo(&info)

	page_size = uint(info.dwPageSize)

	windows.AddVectoredExceptionHandler(1, veh)
}

@(private = "file")
veh :: proc "system" (info: ^windows.EXCEPTION_POINTERS) -> i32 {
	context = runtime.default_context()
	record := info.ExceptionRecord

	if record.ExceptionCode != windows.EXCEPTION_ACCESS_VIOLATION {
		return windows.EXCEPTION_CONTINUE_SEARCH
	}

	if !blocks_initialized do return windows.EXCEPTION_CONTINUE_SEARCH

	fault := uintptr(record.ExceptionInformation[1])

	node := find_le(&blocks, fault)
	if node == nil || fault >= node.value {
		return windows.EXCEPTION_CONTINUE_SEARCH
	}

	page := fault & ~(uintptr(page_size) - 1)
	committed := windows.VirtualAlloc(
		rawptr(page),
		page_size,
		windows.MEM_COMMIT,
		windows.PAGE_READWRITE,
	)
	if committed == nil do return windows.EXCEPTION_CONTINUE_SEARCH

	return windows.EXCEPTION_CONTINUE_EXECUTION
}

_reserve :: proc "contextless" (size: int) -> ([]u8, bool) {
	context = runtime.default_context()
	ensure_state()

	data := windows.VirtualAlloc(nil, uint(size), windows.MEM_RESERVE, windows.PAGE_READWRITE)
	if data == nil do return {}, false

	base := uintptr(data)
	end := base + uintptr(size)

	_, inserted, err := rbtree.find_or_insert(&blocks, base, end)
	if err != nil || !inserted {
		_ = windows.VirtualFree(data, 0, windows.MEM_RELEASE)
		return {}, false
	}

	return (([^]u8)(data))[:size], true
}

_release :: proc "contextless" (block: []u8) {
	context = runtime.default_context()
	base := uintptr(raw_data(block))
	rbtree.remove_key(&blocks, base)
	_ = windows.VirtualFree(raw_data(block), 0, windows.MEM_RELEASE)
}

@(private = "file")
ensure_state :: proc() {
	if blocks_initialized do return
	rbtree.init(&blocks)
	blocks_initialized = true
}

@(private = "file")
find_le :: proc(t: ^Block_Tree, key: uintptr) -> ^rbtree.Node(uintptr, uintptr) {
	node := t._root
	result: ^rbtree.Node(uintptr, uintptr)

	for node != nil {
		switch t._cmp_fn(key, node.key) {
		case .Equal:
			return node

		case .Less:
			node = node._left

		case .Greater:
			result = node
			node = node._right
		}
	}

	return result
}

