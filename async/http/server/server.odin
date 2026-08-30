package async_http_server

import "core:encoding/json"
import "core:mem"
import "core:nbio"
import "core:net"
import "core:strings"
import "core:time"

import "../.."
import cb "../../circular_buffer"
import "../../io"
import "../../storage"

REQUEST_BUFFER_SIZE :: #config(HTTP_SERVER_REQUEST_SIZE, 4 * mem.Kilobyte)
RESPONSE_BUFFER_SIZE :: #config(HTTP_SERVER_RESPONSE_SIZE, 4 * mem.Kilobyte)
LINE_BUFFER_SIZE :: #config(HTTP_SERVER_LINE_SIZE, 4 * mem.Kilobyte)
TEMP_BUFFER_SIZE :: #config(HTTP_SERVER_TEMP_SIZE, 4 * mem.Kilobyte)
CONN_STACK_SIZE :: #config(HTTP_SERVER_STACK_SIZE, 4 * mem.Kilobyte)

Request :: struct {
	method:         string,
	uri:            string,
	version:        string,
	headers:        map[string]string,
	content_length: int,
	//
	socket:         net.TCP_Socket,
	internal:       rawptr,
}

Client :: struct {
	sock: nbio.TCP_Socket,
}

Server :: struct {
	state:           rawptr,
	sock:            net.TCP_Socket,
	clients:         storage.Storage(Client),
	request_handler: proc(state: rawptr, req: ^Request, res: ^Response) -> bool,
	mime_types:      map[string]string,
	open:            bool,
}

init :: proc(
	self: ^Server,
	port: int,
	state: rawptr,
	request_handler: proc(state: rawptr, req: ^Request, res: ^Response) -> bool,
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

@(private = "file")
begin_accept :: proc(self: ^Server) {
	for {
		sock, endpoint, err := io.accept(self.sock)
		if err != nil do break
		net.set_option(sock, .Linger, time.Duration(0))

		client_id := storage.add(&self.clients, Client{sock})
		client_state := Receive_State{self, client_id, sock}
		async.spawn(client_state, begin_receive, stack_size = CONN_STACK_SIZE)
	}
}

@(private = "file")
Receive_State :: struct {
	server: ^Server,
	id:     u64,
	sock:   nbio.TCP_Socket,
}

@(private = "file")
begin_receive :: proc(state: Receive_State) {
	defer {
		io.close(state.sock)
		storage.remove(&state.server.clients, state.id)
	}

	buf, buf_err := make(
		[]u8,
		REQUEST_BUFFER_SIZE + RESPONSE_BUFFER_SIZE + (LINE_BUFFER_SIZE * 2) + TEMP_BUFFER_SIZE,
	)
	if buf_err != nil do return
	defer delete(buf)

	arena: mem.Arena
	mem.arena_init(&arena, buf)
	arena_alloc := mem.arena_allocator(&arena)

	response_buf := make([]u8, RESPONSE_BUFFER_SIZE, arena_alloc)
	response_line_buf := make([]u8, LINE_BUFFER_SIZE, arena_alloc)
	parser_buf := make([]u8, REQUEST_BUFFER_SIZE, arena_alloc)
	line_buf := make([]u8, LINE_BUFFER_SIZE, arena_alloc)
	temp_buf := make([]u8, TEMP_BUFFER_SIZE, arena_alloc)

	parser: Parser
	parser_init(&parser, parser_buf, line_buf)
	defer parser_destroy_request(&parser.req)

	res_internal := Response_Internal {
		sock       = state.sock,
		send_buf   = cb.create(response_buf),
		line_buf   = response_line_buf,
		mime_types = &state.server.mime_types,
	}

	res: Response
	res.internal = &res_internal
	res.headers = make(map[string]string)
	defer delete(res.headers)

	temp_arena: mem.Arena
	mem.arena_init(&temp_arena, temp_buf)
	context.temp_allocator = mem.arena_allocator(&temp_arena)

	for {
		write_slice := parser_peek_write(&parser)
		if len(write_slice) == 0 {
			switch parser.state {
			case .Request_Line:
				res.status = .URI_Too_Long
			case .Headers:
				res.status = .Header_Fields_Too_Large
			case .Body:
				res.status = .Bad_Request
			}
			send_headers(&res)
			break
		}

		n, err := io.recv(state.sock, {write_slice})
		if n == 0 || err != nil do break

		parser_commit_write(&parser, n)

		completed, ok := parser_parse(&parser)
		if !ok {
			res.status = .Bad_Request
			send_headers(&res)
			break
		}

		if !completed do continue

		response_reset(&res)

		{
			defer {
				parser_reset(&parser)
				mem.free_all(context.temp_allocator)
			}

			if ok := state.server.request_handler(state.server.state, &parser.req, &res); !ok do break
		}
	}
}

read :: proc(req: ^Request, dest_buf: []u8) -> (n: int, err: net.Recv_Error) {
	parser := (^Parser)(req.internal)
	if parser.remaining_bytes <= 0 do return 0, nil

	max := min(len(dest_buf), parser.remaining_bytes)
	dst := dest_buf[:max]

	if cb.read(&parser.buf, dst) {
		parser.remaining_bytes -= max
		return max, nil
	}

	n = io.recv(req.socket, {dst}) or_return
	if n == 0 do return 0, nil

	parser.remaining_bytes -= n
	return n, nil
}

get_body_as_text :: proc(req: ^Request, chunk_size: int = 256) -> (string, bool) #optional_ok {
	sb: strings.Builder
	strings.builder_init(&sb)
	defer strings.builder_destroy(&sb)

	buf := make([]u8, chunk_size, context.temp_allocator)
	for {
		n, err := read(req, buf[:])
		if err != nil do return "", false
		if n == 0 do break
		strings.write_bytes(&sb, buf[:n])
	}

	return strings.clone(strings.to_string(sb), context.temp_allocator), true
}

get_body_as_json :: proc(
	req: ^Request,
	$T: typeid,
	chunk_size: int = 256,
) -> (
	value: T,
	ok: bool,
) {
	text := get_body_as_text(req, chunk_size) or_return
	err := json.unmarshal_string(text, &value, allocator = context.temp_allocator)
	if err != nil do return {}, false
	return value, true
}

