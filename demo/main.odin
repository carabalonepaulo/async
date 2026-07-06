package main

import "../async"

main :: proc() {
	async.init()
	defer async.deinit()

	signal_demo()
	select_demo()
	ch_producer_consumer_demo()
	arg_demo()
	producer_consumer_demo()
	http_demo()
	sleep_demo()
}

