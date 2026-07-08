package main

import "../async"
import "../async/io"
import "core:fmt"
import "core:nbio"
import "core:net"
import "core:time"

sock_server :: proc() {
	listener, err := io.listen_tcp({net.IP4_Any, 5059})
	assert(err == nil)

	client, _, accept_err := io.accept(listener)
	assert(accept_err == nil)

	fmt.println("[server] accepted")

	fmt.println("[server] closing client")
	io.close(client)

	io.close(listener)
	fmt.println("[server] closed")
}

sock_client :: proc() {
	endpoint, ok := net.parse_endpoint("127.0.0.1:5059")
	assert(ok)

	client, err := io.dial(endpoint)
	assert(err == nil)

	fmt.println("[client] connected")
	async.sleep(500 * time.Millisecond)

	buf := [256]u8{}

	fmt.println("[client] waiting recv...")
	n, recv_err := io.recv(client, {buf[:]})

	fmt.println("[client] recv returned")
	fmt.println("[client] received:", n)
	fmt.println("[client] err:", recv_err)

	io.close(client)
	fmt.println("[client] closed")
}

sock_demo :: proc() {
	async.spawn(sock_server)
	async.spawn(sock_client)
	async.run(io.poll)
}

