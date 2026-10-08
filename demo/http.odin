package main

import "core:fmt"
import "core:time"

import "../async"
import http "../async/http/client"

coroutine :: proc(client: ^http.Client) {
	out := async.create_chan(http.Result); defer async.destroy(out)

	cancel := async.create_cancel_token()
	async.cancel_after(cancel, 1 * time.Millisecond)

	fmt.println("[http] request sent")
	http.fetch(
		client,
		http.Request {
			method = .Get,
			url = "https://httpbin.org/delay/5",
			out = out,
			cancel = cancel,
		},
	)

	res: http.Result
	idx := async.select({async.branch(out, &res), async.branch(cancel)})

	switch idx {
	case -1:
		fmt.println("[http] select timeout")
	case 0:
		fmt.printfln("[http] request completed, is err %v", res.err != .None)
		http.destroy(res.resp)
	case 1:
		fmt.println("[http] request cancelled (timeout with cancel_after)")
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

	async.schedule(&client, proc(client: ^http.Client) -> bool {
		http.poll(client)
		return true
	})

	async.block(async.spawn(&client, coroutine))
}
