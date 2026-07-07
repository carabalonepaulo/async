package async

init :: proc {
	scheduler_init,
}

deinit :: proc {
	scheduler_deinit,
}

destroy :: proc {
	chan_destroy,
	signal_destroy,
}

try_send :: proc {
	chan_try_send,
}

send :: proc {
	scheduler_send,
	chan_send,
}

try_recv :: proc {
	chan_try_recv,
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

run :: proc {
	scheduler_run,
	scheduler_run_with_poly,
}

