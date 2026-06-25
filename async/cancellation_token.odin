package async

Cancellation_Token :: distinct Chan(Empty)

cancel_token_init :: proc(sched: ^Scheduler, self: ^Cancellation_Token) {
	chan_init(sched, (^Chan(Empty))(self), 0)
}

cancel_token_init_from_coro :: proc(self: ^Cancellation_Token) {
	chan_init_from_coro((^Chan(Empty))(self), 0)
}

cancel_token_branch :: proc(self: ^Cancellation_Token) -> Case {
	return default_branch((^Chan(Empty))(self))
}

trigger :: proc(self: ^Cancellation_Token) {
	chan_deinit((^Chan(Empty))(self))
}

