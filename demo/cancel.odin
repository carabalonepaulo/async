package main

import "../async"
import "core:fmt"
import "core:time"

Signal_Arg :: struct {
	ch:     async.Chan(int),
	cancel: async.Cancellation_Token,
}

signal_producer :: proc(ch: async.Chan(int)) {
	async.sleep(500 * time.Millisecond)
	async.send(ch, 129)
	fmt.println("[producer] sent", 129)
}

signal_select :: proc(arg: Signal_Arg) {
	fmt.println("[select]")
	val: int

	idx := async.select(
		{async.branch(arg.ch, &val), async.branch(arg.cancel)},
		timeout = 1 * time.Second,
	)

	switch idx {
	case -1:
		fmt.println("[select] timeout")
	case 0:
		fmt.println("[select] task finished", val)
	case 1:
		fmt.println("[select] task cancelled")
	}
}

signal_demo :: proc() {
	ch := async.create_chan(int); defer async.destroy(ch)
	cancel := async.create_cancel_token()
	async.trigger(cancel)

	async.spawn(ch, signal_producer)
	async.spawn(Signal_Arg{ch, cancel}, signal_select)

	async.run(1 * time.Millisecond)

	async.clear(ch)
}

