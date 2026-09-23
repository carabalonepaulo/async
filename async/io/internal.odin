package async_io

import ".."
import "core:nbio"

@(private)
store_handle :: #force_inline proc(op: ^nbio.Operation) {
	op.user_data[0] = transmute(rawptr)(async.get_handle())
}

@(private)
load_handle :: #force_inline proc(op: ^nbio.Operation) -> async.Handle {
	return transmute(async.Handle)(op.user_data[0])
}

@(private)
State :: struct($T: typeid) {
	os:     async.One_Shot(T),
	cancel: Maybe(async.Cancel_Token),
}

@(private)
was_cancelled :: proc(cancel: Maybe(async.Cancel_Token)) -> bool {
	cancel, ok := cancel.(async.Cancel_Token)
	if ok do return async.is_triggered(cancel)
	return false
}

