package async

init :: proc {
	scheduler_init,
	chan_init,
	chan_init_from_coro,
	signal_init,
	signal_init_from_coro,
	cancel_token_init,
	cancel_token_init_from_coro,
}

deinit :: proc {
	scheduler_deinit,
	chan_deinit,
	signal_deinit,
}

send :: proc {
	scheduler_send,
	chan_send,
}

recv :: proc {
	scheduler_recv,
	chan_recv,
}

spawn :: proc {
	spawn_with_data,
	spawn_without_data,
}

branch :: proc {
	default_branch,
	signal_branch,
	cancel_token_branch,
}

