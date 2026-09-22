package main

import "../async"
import "../async/io"
import "core:fmt"

@(private = "file")
task :: proc(running: ^bool) {
	it, _ := io.create_read_dir("async")
	defer io.destroy_read_dir(&it)

	for info in io.read_dir(&it) {
		io.read_dir_error(&it) or_continue
		fmt.printfln("[read_dir] file: %v / type: %v", info.name, info.type)
	}

	running^ = false
}

@(private = "file")
sub_task :: proc(running: ^bool) {
	count := 0
	for running^ {
		fmt.println("[read_dir]", count)
		count += 1
		async.reschedule()
	}
}

read_dir_demo :: proc() {
	running := true

	handle_a := async.spawn(&running, task)
	handle_b := async.spawn(&running, sub_task)
	handle_c := async.spawn(handle_a, handle_b, proc(a: async.Handle, b: async.Handle) {
		async.join_many({a, b})
	})

	async.block(handle_c, io.poll)
}

