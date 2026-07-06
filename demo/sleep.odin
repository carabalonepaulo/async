package main

import "../async"

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
	fmt.println("[main] should sleep for 3s")
	async.spawn(3, task)

	fmt.println("[main] should sleep for 5s")
	async.spawn(5, task)

	fmt.println("[main] shoul tick 5 times")
	async.spawn(small_interval)

	for async.get_pending() > 0 {
		async.poll()
		time.sleep(1 * time.Millisecond)
	}

	fmt.println("[main] quit")
}

