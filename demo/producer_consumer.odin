package main

import "../async"
import "core:fmt"
import "core:time"

producer :: proc(handle: async.Handle) {
	for i in 1 ..= 5 {
		fmt.println("[producer] sent", i)
		async.send(handle, i)
		async.reschedule()
	}
}

consumer :: proc() {
	for _ in 0 ..< 5 {
		value := async.recv(int)
		fmt.println("[consumer]", value)
	}
}

producer_consumer_demo :: proc() {
	sched: async.Scheduler
	async.init(&sched)
	defer async.deinit(&sched)

	consumer := async.spawn(&sched, consumer)
	async.spawn(&sched, consumer, producer)

	for async.get_pending(&sched) > 0 {
		async.poll(&sched)
		time.sleep(1 * time.Millisecond)
	}
}

