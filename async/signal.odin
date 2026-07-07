package async

@(private)
Empty :: struct {}

Signal :: distinct Chan(Empty)

create_signal :: proc() -> Signal {
	return (Signal)(create_chan(Empty))
}

signal_branch :: proc(self: Signal) -> Case {
	return default_branch((Chan(Empty))(self))
}

signal_destroy :: proc(self: Signal) {
	chan_destroy((Chan(Empty))(self))
}

emit :: proc(self: Signal) {
	ch := (Chan(Empty))(self)
	for chan_try_send(ch, Empty{}) {}
}

