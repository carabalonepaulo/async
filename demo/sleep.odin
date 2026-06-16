package main

import async "async:scheduler"

import "core:fmt"
import "core:time"

task :: proc(n: int) {
	fmt.printfln("[task] sleeping for %vs...", n)
	async.sleep(time.Duration(n) * time.Second)
	fmt.println("[task] woke up")
}

small_interval :: proc() {
	for i in 0 ..< 5 {
		fmt.println("[task] tick")
		async.sleep(100 * time.Millisecond)
	}
}

sleep_demo :: proc() {
	sched: async.Scheduler
	async.init(&sched)
	defer async.deinit(&sched)

	fmt.println("[main] should sleep for 3s")
	async.spawn(&sched, 3, task)

	fmt.println("[main] should sleep for 5s")
	async.spawn(&sched, 5, task)

	fmt.println("[main] should tick 5 times")
	async.spawn(&sched, small_interval)

	for async.get_pending(&sched) > 0 {
		async.poll(&sched)
		time.sleep(1 * time.Millisecond)
	}

	fmt.println("[main] quit")
}

