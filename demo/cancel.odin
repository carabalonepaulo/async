package main

import "../async"
import "core:fmt"
import "core:time"

Signal_Arg :: struct {
	ch:     ^async.Chan(int),
	cancel: ^async.Chan(bool),
}

signal_producer :: proc(ch: ^async.Chan(int)) {
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
	sched: async.Scheduler
	async.init(&sched)
	defer async.deinit(&sched)

	ch: async.Chan(int)
	cancel: async.Chan(bool)

	async.init(&ch); defer async.deinit(&ch)
	async.init(&cancel); defer async.deinit(&cancel)
	async.send(&cancel, true)

	async.spawn(&sched, &ch, signal_producer)
	async.spawn(&sched, Signal_Arg{&ch, &cancel}, signal_select)

	for async.get_pending(&sched) > 0 {
		async.poll(&sched)
		time.sleep(1 * time.Millisecond)
	}

	async.clear(&ch)
	async.clear(&cancel)
}

