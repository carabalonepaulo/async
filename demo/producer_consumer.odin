package main

import async "async:scheduler"
import "core:fmt"
import "core:time"

producer :: proc(ud: rawptr) {
	handle := (^async.Handle)(ud)^
	for i in 1 ..= 5 {
		async.sleep(1 * time.Second)
		async.send(handle, i)
		fmt.println("[producer] sent", i)
	}
}

consumer :: proc(_: rawptr) {
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
	async.spawn(&sched, producer, &consumer)

	for async.get_pending(&sched) > 0 {
		async.poll(&sched)
		time.sleep(1 * time.Millisecond)
	}
}

