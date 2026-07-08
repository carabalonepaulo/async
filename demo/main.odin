package main

import "../async"
import "../async/io"

main :: proc() {
	async.init()
	defer async.deinit()

	io.init()
	defer io.deinit()

	sock_demo()
	// fs_demo()
	// signal_demo()
	// select_demo()
	// ch_producer_consumer_demo()
	// arg_demo()
	// producer_consumer_demo()
	// http_demo()
	// sleep_demo()
}

