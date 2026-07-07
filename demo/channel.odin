package main

import "../async"
import "core:fmt"
import "core:time"

Arg :: struct {
	consumer_handle: async.Handle,
	ch:              async.Chan(int),
}

ch_producer :: proc(arg: Arg) {
	for i in 1 ..= 5 {
		fmt.println("[producer] sent", i)
		async.send(arg.ch, i)
		if i % 2 == 0 do async.reschedule()
	}
}

ch_consumer :: proc(ch: async.Chan(int)) {
	for _ in 0 ..< 5 {
		value, ok := async.recv(ch)
		fmt.println("[consumer]", value, ok)
	}
}

ch_producer_consumer_demo :: proc() {
	ch := async.create_chan(int); defer async.destroy(ch)

	consumer := async.spawn(ch, ch_consumer)
	async.spawn(Arg{consumer, ch}, ch_producer)

	async.run(1 * time.Millisecond)
	async.clear(ch)
}

