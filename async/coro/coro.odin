package coro

import "core:c"
import "core:strings"

foreign import minicoro "minicoro.lib"

Coro :: struct {}

Desc :: struct {
	func:           proc "c" (co: ^Coro),
	user_data:      rawptr,
	alloc_cb:       proc "c" (size: c.size_t, allocator_data: rawptr) -> rawptr,
	dealloc_cb:     proc "c" (ptr: rawptr, size: c.size_t, allocator_data: rawptr),
	allocator_data: rawptr,
	storage_size:   c.size_t,
	coro_size:      c.size_t,
	stack_size:     c.size_t,
}

State :: enum c.int {
	Dead      = 0,
	Normal    = 1,
	Running   = 2,
	Suspended = 3,
}

Result :: enum c.int {
	Success              = 0,
	Generic_Error        = 1,
	Invalid_Pointer      = 2,
	Invalid_Coroutine    = 3,
	Not_Suspended        = 4,
	Not_Running          = 5,
	Make_Context_Error   = 6,
	Switch_Context_Error = 7,
	Not_Enough_Space     = 8,
	Out_Of_Memory        = 9,
	Invalid_Arguments    = 10,
	Invalid_Operation    = 11,
	Stack_Overflow       = 12,
}

@(default_calling_convention = "c", link_prefix = "mco_")
foreign minicoro {
	desc_init :: proc(func: proc "c" (co: ^Coro), stack_size: c.size_t) -> Desc ---
	init :: proc(co: ^Coro, desc: ^Desc) -> Result ---
	uninit :: proc(co: ^Coro) -> Result ---
	create :: proc(co: ^^Coro, desc: ^Desc) -> Result ---
	destroy :: proc(co: ^Coro) -> Result ---
	resume :: proc(co: ^Coro) -> Result ---
	yield :: proc(co: ^Coro) -> Result ---
	status :: proc(co: ^Coro) -> State ---
	get_user_data :: proc(co: ^Coro) -> rawptr ---

	push :: proc(co: ^Coro, src: rawptr, len: c.size_t) -> Result ---
	pop :: proc(co: ^Coro, dest: rawptr, len: c.size_t) -> Result ---
	peek :: proc(co: ^Coro, dest: rawptr, len: c.size_t) -> Result ---
	get_storage_size :: proc(co: ^Coro) -> c.size_t ---
	get_bytes_stored :: proc(co: ^Coro) -> c.size_t ---

	running :: proc() -> ^Coro ---
	result_description :: proc(res: Result) -> cstring ---
}

check :: #force_inline proc(res: Result) -> Result {
	if res != .Success do panic(strings.clone_from_cstring(result_description(res)))
	return res
}

