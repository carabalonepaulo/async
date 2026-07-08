package async_io_internal

import "../../"
import "core:nbio"

store_handle :: #force_inline proc(op: ^nbio.Operation) {
	op.user_data[0] = transmute(rawptr)(async.get_handle())
}

load_handle :: #force_inline proc(op: ^nbio.Operation) -> async.Handle {
	return transmute(async.Handle)(op.user_data[0])
}

