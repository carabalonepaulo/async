package async

Cancellation_Token :: distinct Chan(Empty)

cancel_token_init :: proc(self: ^Cancellation_Token) {
	chan_init((^Chan(Empty))(self), 0)
}

cancel_token_branch :: proc(self: ^Cancellation_Token) -> Case {
	return default_branch((^Chan(Empty))(self))
}

trigger :: proc(self: ^Cancellation_Token) {
	chan_deinit((^Chan(Empty))(self))
}

