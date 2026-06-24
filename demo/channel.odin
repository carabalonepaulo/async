package main

import "async:chan"
import async "async:scheduler"
import "core:fmt"
import "core:time"

Arg :: struct {
	consumer_handle: async.Handle,
	ch:              ^chan.Chan(int),
}

ch_producer :: proc(arg: Arg) {
	for i in 1 ..= 5 {
		fmt.println("[producer] sent", i)
		chan.send(arg.ch, i)
		if i % 2 == 0 do async.reschedule()
	}
}

ch_consumer :: proc(ch: ^chan.Chan(int)) {
	for _ in 0 ..< 5 {
		value, ok := chan.recv(ch)
		fmt.println("[consumer]", value, ok)
	}
}

ch_producer_consumer_demo :: proc() {
	sched: async.Scheduler
	async.init(&sched)
	defer async.deinit(&sched)

	ch: chan.Chan(int)
	chan.init(&ch)
	defer chan.deinit(&ch)

	consumer := async.spawn(&sched, &ch, ch_consumer)
	async.spawn(&sched, Arg{consumer, &ch}, ch_producer)

	for async.get_pending(&sched) > 0 {
		async.poll(&sched)
		time.sleep(1 * time.Millisecond)
	}
}

