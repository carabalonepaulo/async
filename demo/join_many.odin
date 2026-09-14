package main

import "../async"
import "core:fmt"
import "core:time"

@(private = "file")
child_task :: proc() {
	async.sleep(1 * time.Second)
}

@(private = "file")
parent_task :: proc() {
	tasks := make([dynamic]async.Handle)
	defer delete(tasks)

	count := 5
	fmt.printfln("[join_many] spawning %v tasks", count)
	for _ in 0 ..< count do append(&tasks, async.spawn(child_task))

	fmt.println("[join_many] before join")
	async.join_many(tasks[:])
	fmt.println("[join_many] after join")
}

join_many_demo :: proc() {
	async.spawn(parent_task)
	async.run(1 * time.Millisecond)
}

