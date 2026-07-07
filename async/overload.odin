package async

init :: proc {
	scheduler_init,
// chan_init,
// signal_init,
// cancel_token_init,
}

deinit :: proc {
	scheduler_deinit,
// chan_deinit,
// signal_deinit,
}

destroy :: proc {
	chan_destroy,
	signal_destroy,
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

run :: proc {
	scheduler_run,
	scheduler_run_with_poly,
}

