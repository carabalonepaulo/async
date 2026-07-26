package async_http_server

import "core:fmt"
import "core:mem"
import "core:nbio"
import "core:net"

import "../.."
import "../../io"
import "../../storage"

BUFFER_SIZE :: 4096

Request :: struct {
	method:         string,
	uri:            string,
	version:        string,
	headers:        map[string]string,
	body:           []u8,
	content_length: int,
}

Client :: struct {
	sock: net.TCP_Socket,
}

Server :: struct {
	state:           rawptr,
	sock:            net.TCP_Socket,
	clients:         storage.Storage(Client),
	request_handler: proc(state: rawptr, req: ^Request, res: ^Response),
	mime_types:      map[string]string,
	open:            bool,
}

init :: proc(
	self: ^Server,
	port: int,
	state: rawptr,
	request_handler: proc(state: rawptr, req: ^Request, res: ^Response),
) -> (
	err: net.Network_Error,
) {
	endpoint := net.Endpoint{net.IP4_Any, port}
	self.sock = io.listen_tcp(endpoint) or_return
	self.state = state
	self.open = true
	self.request_handler = request_handler

	self.mime_types = make(map[string]string)
	init_mime_types(&self.mime_types)

	storage.init(&self.clients)
	async.spawn(self, begin_accept, stack_size = 64)
	return nil
}

deinit :: proc(self: ^Server) {
	if !self.open do return
	self.open = false

	storage.retain(&self.clients, nil, proc(_: u64, client: ^Client, _: rawptr) -> bool {
		net.close(client.sock)
		return false
	})
	storage.deinit(&self.clients)
	net.close(self.sock)

	deinit_mime_types(&self.mime_types)
}

begin_accept :: proc(self: ^Server) {
	for {
		sock, endpoint, err := io.accept(self.sock)
		if err != nil do break

		client_id := storage.add(&self.clients, Client{sock})
		client_state := Receive_State{self, client_id}
		async.spawn(client_state, begin_receive)
	}
}

Receive_State :: struct {
	server:    ^Server,
	client_id: u64,
}

begin_receive :: proc(state: Receive_State) {
	parser: Parser
	parser_init(&parser)
	defer parser_deinit(&parser)

	res: Response
	res.headers = make(map[string]string)
	defer delete(res.headers)

	temp_buf := make([]u8, BUFFER_SIZE)
	defer delete(temp_buf)

	arena: mem.Arena
	mem.arena_init(&arena, temp_buf)
	context.temp_allocator = mem.arena_allocator(&arena)

	outer: for {
		client, ok := storage.get_ptr(&state.server.clients, state.client_id)
		if !ok do break

		write_slice := parser_get_write_slice(&parser)
		if len(write_slice) == 0 do break

		n, err := io.recv(client.sock, {write_slice})
		if n == 0 || err != nil do break

		parser_commit_write(&parser, n)

		for {
			completed, failed := parser_parse(&parser)
			if failed {
				res.status = .Bad_Request
				response_send(state.server, client, &res)
				break outer
			}

			if !completed do break

			response_reset(&res)
			state.server.request_handler(nil, &parser.req, &res)

			if !response_send(state.server, client, &res) do break outer

			parser_reset(&parser)
			mem.free_all(context.temp_allocator)
		}
	}

	storage.remove(&state.server.clients, state.client_id)
}

