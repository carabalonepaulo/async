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
	consumer := async.spawn(consumer)
	async.spawn(consumer, producer)

	for async.get_pending() > 0 {
		async.poll()
		time.sleep(1 * time.Millisecond)
	}
}

