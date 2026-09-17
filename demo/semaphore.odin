package main

import "../async"
import "core:fmt"
import "core:time"

@(private = "file")
child :: proc(sem: async.Semaphore, cancel: async.Cancel_Token, count: ^int) {
	for _ in 0 ..< 3 {
		async.guard(sem, cancel) or_break
		count^ += 1
		fmt.printfln("[semaphore] child %v", count^)
		async.sleep_or_cancel(1 * time.Second, cancel) or_break
	}
}

@(private = "file")
parent :: proc() {
	PERMITS :: 4
	TASKS :: 10

	cancel := async.create_cancel_token()
	async.cancel_after(cancel, 1 * time.Second)

	sem := async.create_semaphore(PERMITS)
	defer async.semaphore_destroy(sem)

	children := new([TASKS]async.Handle)
	defer free(children)

	count := 0
	for i in 0 ..< TASKS do children[i] = async.spawn(sem, cancel, &count, child)

	async.join_many(children[:])
}

semaphore_demo :: proc() {
	async.spawn(parent)
	async.run(1 * time.Millisecond)
}

