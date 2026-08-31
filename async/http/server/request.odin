package async_http_server

import "core:encoding/json"
import "core:nbio"
import "core:net"
import "core:strings"

import cb "../../circular_buffer"
import "../../io"

Method :: enum {
	Get,
	Head,
	Post,
	Put,
	Delete,
	Connect,
	Options,
	Trace,
	Patch,
}

Request :: struct {
	method:         Method,
	// method:         string,
	uri:            string,
	version:        string,
	headers:        map[string]string,
	content_length: int,
	//
	socket:         nbio.TCP_Socket,
	internal:       rawptr,
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

