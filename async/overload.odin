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
	wait_group_destroy,
	semaphore_destroy,
	one_shot_destroy,
}

try_send :: proc {
	chan_try_send,
	one_shot_try_send,
}

send :: proc {
	chan_send,
	one_shot_send,
}

try_recv :: proc {
	chan_try_recv,
	one_shot_try_recv,
}

recv :: proc {
	chan_recv,
	one_shot_recv,
}

spawn :: proc {
	spawn_with_poly,
	spawn_with_poly2,
	spawn_with_poly3,
	spawn_with_poly4,
	spawn_with_poly5,
	spawn_without_data,
}

branch :: proc {
	chan_branch,
	signal_branch,
	cancel_token_branch,
	semaphore_branch,
	one_shot_branch,
}

run :: proc {
	scheduler_run,
	scheduler_run_with,
	scheduler_run_with_poly,
}

block :: proc {
	scheduler_block,
	scheduler_block_with,
	scheduler_block_with_poly,
}

into_rawptr :: proc {
	handle_into_rawptr,
	chan_into_rawptr,
	cancel_token_into_rawptr,
	signal_into_rawptr,
}

wait :: proc {
	cancel_token_wait,
	wait_group_wait,
}

clear :: proc {
	chan_clear,
}

len :: proc {
	chan_len,
}

