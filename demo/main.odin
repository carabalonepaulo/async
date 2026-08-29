package main

import "../async"
import "../async/io"
import "core:fmt"
import "core:mem"

main :: proc() {
	track: mem.Tracking_Allocator
	mem.tracking_allocator_init(&track, context.allocator)
	context.allocator = mem.tracking_allocator(&track)
	defer {
		if len(track.allocation_map) > 0 {
			fmt.println("[track] leaks")
			for _, value in track.allocation_map {
				fmt.printfln("- %v bytes at %v", value.size, value.location)
			}
		}

		if len(track.bad_free_array) > 0 {
			fmt.println("[track] bad free")
			for value in track.bad_free_array {
				fmt.printfln("- %v", value.location)
			}
		}

		mem.tracking_allocator_destroy(&track)
	}

	async.init()
	defer async.deinit()

	io.init()
	defer io.deinit()

	http_server_demo()
	// sock_demo()
	// fs_demo()
	// signal_demo()
	// select_demo()
	// ch_producer_consumer_demo()
	// arg_demo()
	// producer_consumer_demo()
	// http_demo()
	// sleep_demo()
}

