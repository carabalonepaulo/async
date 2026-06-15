package main

import "async:http"
import async "async:scheduler"

import "core:fmt"
import "core:time"

coroutine :: proc(ud: rawptr) {
	client := (^http.Client)(ud)
	resp, err := http.get(client, "https://jsonplaceholder.typicode.com/posts/1")
	defer if err == .None do http.destroy(resp)

	if err == .None {
		fmt.printfln("status: %v", resp.status)
		fmt.printfln("headers: %v", resp.headers[:])
		fmt.printfln("body: %v", string(resp.body[:]))
	} else {
		fmt.printfln("request failed with error: %v", err)
	}
}

http_demo :: proc() {
	sched: async.Scheduler
	async.init(&sched)
	defer async.deinit(&sched)

	client: http.Client
	init_err := http.init(&client)
	if init_err != .None {
		fmt.printfln("failed to init http client: %v", init_err)
		return
	}
	defer http.deinit(&client)

	async.spawn(&sched, coroutine, &client)

	for async.get_pending(&sched) > 0 {
		async.poll(&sched)
		http.poll(&client)
		time.sleep(1 * time.Millisecond)
	}
}

