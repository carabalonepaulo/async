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
	scheduler_run_with,
	scheduler_run_with_poly,
}

get_user_data :: proc {
	get_user_data_from_current,
	get_user_data_from_handle,
}

set_user_data :: proc {
	set_user_data_to_current,
	set_user_data_to_handle,
}

into_rawptr :: proc {
	handle_into_rawptr,
	chan_into_rawptr,
	cancel_token_into_rawptr,
	signal_into_rawptr,
}

