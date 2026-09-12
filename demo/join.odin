package main

import "../async"
import "core:fmt"
import "core:time"

join_task_a :: proc() {
	async.sleep(3 * time.Second)
}

join_task_b :: proc(a: async.Handle) {
	fmt.println("[join] before join")
	async.join(a)
	fmt.println("[join] after join")
}

join_demo :: proc() {
	a := async.spawn(join_task_a)
	async.spawn(a, join_task_b)
	async.run(1 * time.Millisecond)
}

