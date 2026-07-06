package main

import "../async"
import "../async/http"

import "core:fmt"
import "core:time"

coroutine :: proc(client: ^http.Client) {
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
	client: http.Client
	init_err := http.init(&client)
	if init_err != .None {
		fmt.printfln("failed to init http client: %v", init_err)
		return
	}
	defer http.deinit(&client)

	async.spawn(&client, coroutine)
	async.run(&client, http.poll, 1 * time.Millisecond)
}

