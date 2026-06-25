package main

import "async:chan"
import async "async:scheduler"
import "core:container/queue"
import "core:fmt"
import "core:time"

Select_Arg :: struct {
	ch_a: ^chan.Chan(int),
	ch_b: ^chan.Chan(int),
}

producer_a :: proc(ch: ^chan.Chan(int)) {
	async.sleep(5 * time.Second)
	fmt.println("[A] sent", 3)
	chan.send(ch, 3)
	fmt.println("[A] end")
}

producer_b :: proc(ch: ^chan.Chan(int)) {
	async.sleep(3 * time.Second)
	fmt.println("[B] sent", 5)
	chan.send(ch, 5)
	fmt.println("[B] end")
}

consumer_select :: proc(arg: Select_Arg) {
	for i in 0 ..< 2 {
		fmt.println("[select]", i)
		a_val: int
		b_val: int

		idx := chan.select(
			{chan.branch(arg.ch_a, &a_val), chan.branch(arg.ch_b, &b_val)},
			timeout = 1 * time.Second,
		)

		switch idx {
		case -1:
			fmt.println("[select] timeout")
		case 0:
			fmt.println("[select] A won", a_val)
		case 1:
			fmt.println("[select] B won", b_val)
		}
	}
	fmt.println("[select] end")
}

select_demo :: proc() {
	sched: async.Scheduler
	async.init(&sched)
	defer async.deinit(&sched)

	ch_a: chan.Chan(int)
	ch_b: chan.Chan(int)

	chan.init(&ch_a); defer chan.deinit(&ch_a)
	chan.init(&ch_b); defer chan.deinit(&ch_b)

	async.spawn(&sched, &ch_a, producer_a)
	async.spawn(&sched, &ch_b, producer_b)
	async.spawn(&sched, Select_Arg{&ch_a, &ch_b}, consumer_select)

	for async.get_pending(&sched) > 0 {
		async.poll(&sched)
		time.sleep(1 * time.Millisecond)
	}

	for ch_a.items.len > 0 do queue.pop_front(&ch_a.items)
	for ch_b.items.len > 0 do queue.pop_front(&ch_b.items)
}

