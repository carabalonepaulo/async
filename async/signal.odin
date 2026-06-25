package async

import "core:container/queue"

@(private)
Empty :: struct {}

Signal :: distinct Chan(Empty)

signal_init :: proc(sched: ^Scheduler, self: ^Signal) {
	chan_init(sched, (^Chan(Empty))(self))
}

signal_init_from_coro :: proc(self: ^Signal) {
	chan_init_from_coro((^Chan(Empty))(self))
}

signal_branch :: proc(self: ^Signal) -> Case {
	return default_branch((^Chan(Empty))(self))
}

signal_deinit :: proc(self: ^Signal) {
	chan_deinit((^Chan(Empty))(self))
}

emit :: proc(self: ^Signal) {
	ch := (^Chan(Empty))(self)
	count := queue.len(ch.receivers)
	for _ in 0 ..< count do chan_send(ch, Empty{})
}

