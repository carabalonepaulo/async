package async_http_server

import "core:mem"
import "core:nbio"
import "core:net"
import "core:time"

import "../.."
import cb "../../circular_buffer"
import "../../io"
import "../../storage"
import "headers"

REQUEST_BUFFER_SIZE :: #config(HTTP_SERVER_REQUEST_SIZE, 4 * mem.Kilobyte)
RESPONSE_BUFFER_SIZE :: #config(HTTP_SERVER_RESPONSE_SIZE, 4 * mem.Kilobyte)
LINE_BUFFER_SIZE :: #config(HTTP_SERVER_LINE_SIZE, 4 * mem.Kilobyte)
TEMP_BUFFER_SIZE :: #config(HTTP_SERVER_TEMP_SIZE, 4 * mem.Kilobyte)
CONN_STACK_SIZE :: #config(HTTP_SERVER_STACK_SIZE, 4 * mem.Kilobyte)

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
	res.headers = headers.Headers(make([dynamic]headers.Header))
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
			fail(&res) or_break
		}

		n, err := io.recv(state.sock, {write_slice})
		if n == 0 || err != nil do break

		parser_commit_write(&parser, n)

		parse_result := parser_parse(&parser)
		#partial switch parse_result {
		case .Partial:
			continue
		case .Invalid_Method:
			fail(&res, .Not_Implemented) or_break
		case .Invalid_HTTP_Version:
			fail(&res, .HTTP_Version_Not_Supported) or_break
		case .Invalid_Request_Line, .Invalid_Content_Length:
			fail(&res, .Bad_Request) or_break
		}

		response_reset(&res)
		(^Response_Internal)(res.internal).method = parser.req.method

		{
			defer {
				parser_reset(&parser)
				mem.free_all(context.temp_allocator)
			}

			if ok := state.server.request_handler(state.server.state, &parser.req, &res); !ok do break
		}
	}
}

@(private)
fail :: proc(res: ^Response, status: Maybe(Status) = nil) -> bool {
	if status, ok := status.(Status); ok do res.status = status
	headers.add(&res.headers, "Content-Length", "0")
	send_headers(res)
	return false
}

