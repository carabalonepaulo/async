package async

Cancellation_Token :: distinct Chan(Empty)

create_cancel_token :: proc() -> Cancellation_Token {
	return (Cancellation_Token)(create_chan(Empty, 0))
}

cancel_token_branch :: proc(self: Cancellation_Token) -> Case {
	return default_branch((Chan(Empty))(self))
}

trigger :: proc(self: Cancellation_Token) {
	chan_destroy((Chan(Empty))(self))
}

