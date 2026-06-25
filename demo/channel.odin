package main

import "../async"
import "core:fmt"
import "core:time"

Arg :: struct {
	consumer_handle: async.Handle,
	ch:              ^async.Chan(int),
}

ch_producer :: proc(arg: Arg) {
	for i in 1 ..= 5 {
		fmt.println("[producer] sent", i)
		async.send(arg.ch, i)
		if i % 2 == 0 do async.reschedule()
	}
}

ch_consumer :: proc(ch: ^async.Chan(int)) {
	for _ in 0 ..< 5 {
		value, ok := async.recv(ch)
		fmt.println("[consumer]", value, ok)
	}
}

ch_producer_consumer_demo :: proc() {
	sched: async.Scheduler
	async.init(&sched)
	defer async.deinit(&sched)

	ch: async.Chan(int)
	async.init(&ch)
	defer async.deinit(&ch)

	consumer := async.spawn(&sched, &ch, ch_consumer)
	async.spawn(&sched, Arg{consumer, &ch}, ch_producer)

	for async.get_pending(&sched) > 0 {
		async.poll(&sched)
		time.sleep(1 * time.Millisecond)
	}
}

