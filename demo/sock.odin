package main

import "../async"
import "../async/io"
import "../async/storage"

import "core:fmt"
import "core:nbio"
import "core:net"
import "core:time"

@(private)
Args :: struct {
	clients: ^storage.Storage(net.TCP_Socket),
	client:  net.TCP_Socket,
	id:      u64,
}

sock_server :: proc() {
	listener, listen_err := io.listen_tcp({nbio.IP4_Any, 5059})
	assert(listen_err == nil)
	fmt.println("[sock:server] listening")

	clients: storage.Storage(net.TCP_Socket)
	storage.init(&clients)

	client, client_endpoint, accept_err := io.accept(listener)
	if accept_err == .None {
		fmt.println("[sock:server] client accepted")
		id := storage.add(&clients, client)
		async.spawn(Args{&clients, client, id}, sock_listener_client)
	} else {
		fmt.println("[sock:server] accept failed")
	}

	for {
		async.sleep(time.Millisecond)
		if storage.count(&clients) == 0 do break
	}

	io.close(listener)
	fmt.println("[sock:server] listener closed")
}

sock_listener_client :: proc(args: Args) {
	clients, client, id := args.clients, args.client, args.id
	fmt.printfln("[sock:server:client:%v] client loop", id)

	buf := [256]u8{}
	count := 0

	for count < 3 {
		fmt.printfln("[sock:server:client:%v] receiving...", id)
		received, recv_err := io.recv(client, {buf[:]})
		if received == 0 || recv_err != nil {
			fmt.printfln("[sock:server:client:%v] recv failed", id)
			break
		}
		fmt.printfln("[sock:server:client:%v] %v bytes received", id, received)

		fmt.printfln("[sock:server:client:%v] sending...", id)
		sent, send_err := io.send(client, {buf[:received]})
		if sent != received || send_err != nil {
			fmt.printfln("[sock:server:client:%v] send failed", id)
			break
		}
		fmt.printfln("[sock:server:client:%v] %v bytes sent", id, sent)

		count += 1
	}

	fmt.printfln("[sock:server:client:%v] closing with count %v", id, count)
	io.close(client)
	storage.remove(clients, id)
}

sock_client :: proc() {
	endpoint, ok := net.parse_endpoint("127.0.0.1:5059")
	assert(ok, "failed to parse endpoint")

	client, connect_err := io.dial(endpoint, 5 * time.Second)
	if connect_err != nil {
		fmt.println("[sock:client] connect failed")
		return
	}
	fmt.println("[sock:client] connected")

	data := "hello world!"
	buf := [256]u8{}
	count := 0

	for count < 3 {
		fmt.println("[sock:client] sending...")
		sent, send_err := io.send(client, {transmute([]u8)(data)})
		if send_err != nil {
			fmt.println("[sock:client] send failed")
			break
		}
		fmt.printfln("[sock:client] %v/%v bytes sent", sent, len(data))

		fmt.println("[sock:client] receiving...")
		received, recv_err := io.recv(client, {buf[:]})
		if received == 0 || recv_err != nil {
			fmt.println("[sock:client] recv failed")
			break
		}
		fmt.printfln("[sock:client] %v bytes received", received)

		count += 1
	}

	io.close(client)
	fmt.printfln("[sock:client] client closed with count %v", count)
}

sock_demo :: proc() {
	async.spawn(sock_server)
	async.spawn(sock_client)
	async.run(io.poll)
}

