package scheduler

init :: proc {
	scheduler_init,
	chan_init,
}

deinit :: proc {
	scheduler_deinit,
	chan_deinit,
}

send :: proc {
	scheduler_send,
	chan_send,
}

recv :: proc {
	scheduler_recv,
	chan_recv,
}

