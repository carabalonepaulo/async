package async

import "core:mem"
@(private)
Empty :: struct {}

Signal :: distinct Chan(Empty)

create_signal :: proc() -> (signal: Signal, err: mem.Allocator_Error) {
	ch := create_chan(Empty) or_return
	return (Signal)(ch), .None
}

signal_branch :: proc(self: Signal) -> Case {
	return chan_branch((Chan(Empty))(self))
}

signal_destroy :: proc(self: Signal) {
	chan_destroy((Chan(Empty))(self))
}

emit :: proc(self: Signal) {
	ch := (Chan(Empty))(self)
	for chan_try_send(ch, Empty{}) {}
}

signal_into_rawptr :: #force_inline proc(self: Signal) -> rawptr {
	return transmute(rawptr)(self.id)
}

signal_from_rawptr :: #force_inline proc(ptr: rawptr) -> Signal {
	return (Signal)(Chan(Empty){id = transmute(u64)(ptr)})
}
