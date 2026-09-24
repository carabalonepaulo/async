package async_http_server

import "core:mem"
import "core:nbio"
import "core:net"
import "core:sync/chan"

import "../.."
import cb "../../circular_buffer"
import "../../io"
import "headers"

Receive_State :: struct {
	mime_types:      map[string]string,
	sock:            net.TCP_Socket,
	shared_state:    rawptr,
	request_handler: Request_Handler,
}

worker :: proc(id: int, msgs: chan.Chan(Receive_State), should_close: ^bool) {
	cancel_tokens := make(map[async.Cancel_Token]bool)
	defer delete(cancel_tokens)

	async.init()
	defer async.deinit()

	io.init()
	defer io.deinit()

	cancel_all :: proc(cancel_tokens: ^map[async.Cancel_Token]bool) {
		for tk in cancel_tokens do async.trigger(tk)
		clear(cancel_tokens)
	}

	for {
		msg := chan.recv(msgs) or_break
		async.spawn(msg, &cancel_tokens, begin_receive)

		for {
			async.poll()
			io.poll()
			if should_close^ do cancel_all(&cancel_tokens)
			for msg in chan.try_recv(msgs) do async.spawn(msg, &cancel_tokens, begin_receive)
			if async.get_pending() == 0 do break
		}
	}
}

@(private = "file")
begin_receive :: proc(state: Receive_State, cancel_tokens: ^map[async.Cancel_Token]bool) {
	cancel_token := async.create_cancel_token()
	cancel_tokens[cancel_token] = true

	defer {
		io.close(state.sock)
		delete_key(cancel_tokens, cancel_token)
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

	mime_types := state.mime_types
	res_internal := Response_Internal {
		sock       = state.sock,
		cancel     = cancel_token,
		send_buf   = cb.create(response_buf),
		line_buf   = response_line_buf,
		mime_types = &mime_types,
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

		n, err := io.recv(state.sock, {write_slice}, cancel = cancel_token)
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

			if ok := state.request_handler(state.shared_state, &parser.req, &res); !ok do break
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

