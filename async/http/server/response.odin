package async_http_server

import "core:encoding/json"
import "core:fmt"
import "core:nbio"
import "core:net"
import "core:strconv"
import "core:strings"

import cb "../../circular_buffer"
import "../../io"

@(private)
Response_Internal :: struct {
	sock:       nbio.TCP_Socket,
	send_buf:   cb.Circular_Buffer,
	line_buf:   []u8,
	mime_types: ^map[string]string,
}

Response :: struct {
	status:   Status,
	headers:  map[string]string,
	internal: rawptr,
}

@(private)
response_reset :: proc(res: ^Response) {
	res.status = .Ok
	clear(&res.headers)

	internal := (^Response_Internal)(res.internal)
	cb.clear(&internal.send_buf)
}

send_headers :: proc(res: ^Response) -> (ok: bool) {
	internal := (^Response_Internal)(res.internal)
	send_buf := &internal.send_buf
	line_buf := internal.line_buf

	line := fmt.bprintf(
		line_buf,
		"HTTP/1.1 %d %s\r\n",
		int(res.status),
		get_status_text(res.status),
	)
	if !cb.write(send_buf, transmute([]u8)(line)) do return false

	res.headers["Connection"] = "keep-alive"

	for k, v in res.headers {
		line = fmt.bprintf(line_buf, "%s: %s\r\n", k, v)
		if !cb.can_write(send_buf, len(line)) do flush(internal.sock, send_buf) or_return
		cb.write(send_buf, transmute([]u8)(line))
	}

	if !cb.can_write(send_buf, 2) do flush(internal.sock, send_buf) or_return
	cb.write(send_buf, {'\r', '\n'})
	return flush(internal.sock, send_buf)
}

send :: proc(res: ^Response, buf: []u8) -> (ok: bool) {
	internal := (^Response_Internal)(res.internal)
	send_buf := &internal.send_buf

	res.headers["Content-Length"] = fmt.tprint(len(buf))
	send_headers(res) or_return

	_send_buf(internal.sock, buf, send_buf)

	if send_buf.ra > 0 do flush(internal.sock, send_buf) or_return
	return true
}

send_text :: proc(res: ^Response, status: Status, text: string) -> (ok: bool) {
	res.headers["Content-Type"] = "text/plain; charset=utf-8"
	res.status = status
	return send(res, transmute([]u8)(text))
}

send_file :: proc(req: ^Request, res: ^Response, file_path: string) -> (ok: bool) {
	internal := (^Response_Internal)(res.internal)

	file, open_err := io.open(file_path, {.Read})
	if open_err != nil do return send_text(res, .Not_Found, "")

	type, size, stat_err := io.stat(file)
	if stat_err != nil || type != .Regular do return send_text(res, .Internal_Server_Error, "")

	if "Content-Type" not_in res.headers {
		res.headers["Content-Type"] = get_mime_type_from_path(internal.mime_types, file_path)
	}
	res.headers["Accept-Ranges"] = "bytes"

	offset: int = 0
	length: int = int(size)

	if range_header, has_range := req.headers["Range"]; has_range {
		start, end, valid := parse_range_header(range_header, int(size))
		if valid {
			res.status = .Partial_Content
			offset = start
			length = (end - start) + 1
			res.headers["Content-Range"] = fmt.tprintf("bytes %d-%d/%d", start, end, size)
		} else {
			res.status = .Range_Not_Satisfiable
			res.headers["Content-Range"] = fmt.tprintf("bytes */%d", size)
			res.headers["Content-Length"] = "0"
			return send_headers(res)
		}
	}

	res.headers["Content-Length"] = fmt.tprint(length)
	send_headers(res) or_return

	cb :: proc(op: ^nbio.Operation) {nbio.close(op.sendfile.file)}
	nbio.sendfile(internal.sock, file, cb, offset, length)

	return true
}

send_json :: proc(res: ^Response, value: $T) -> (ok: bool) {
	buf, json_err := json.marshal(value, allocator = context.temp_allocator)
	if json_err != nil {
		res.status = .Internal_Server_Error
		res.headers["Content-Length"] = "0"
		return send_headers(res)
	}

	res.headers["Content-Type"] = "application/json"
	return send(res, buf)
}

begin_chunked :: proc(res: ^Response, status: Status = .Ok) -> (ok: bool) {
	res.status = status
	res.headers["Transfer-Encoding"] = "chunked"
	delete_key(&res.headers, "Content-Length")
	send_headers(res) or_return
	return true
}

send_chunk :: proc(res: ^Response, buf: []u8) -> (ok: bool) {
	if len(buf) == 0 do return true

	internal := (^Response_Internal)(res.internal)
	send_buf := &internal.send_buf
	line_buf := internal.line_buf

	line := fmt.bprintf(line_buf, "%x\r\n", len(buf))

	if !cb.can_write(send_buf, len(line)) do flush(internal.sock, send_buf) or_return
	cb.write(send_buf, transmute([]u8)line)

	_send_buf(internal.sock, buf, send_buf)

	if !cb.can_write(send_buf, 2) do flush(internal.sock, send_buf) or_return
	cb.write(send_buf, {'\r', '\n'})

	return flush(internal.sock, send_buf)
}

end_chunked :: proc(res: ^Response) -> (ok: bool) {
	internal := (^Response_Internal)(res.internal)
	send_buf := &internal.send_buf

	if !cb.can_write(send_buf, 5) do flush(internal.sock, send_buf) or_return
	cb.write(send_buf, {'0', '\r', '\n', '\r', '\n'})

	return flush(internal.sock, send_buf)
}

@(private)
_send_buf :: proc(sock: nbio.TCP_Socket, buf: []u8, send_buf: ^cb.Circular_Buffer) -> (ok: bool) {
	remaining := buf
	for len(remaining) > 0 {
		send_len := min(len(remaining), send_buf.wa)
		if send_len > 0 {
			cb.write(send_buf, remaining[:send_len])
			remaining = remaining[send_len:]
		}
		flush(sock, send_buf) or_return
	}
	return true
}

@(private)
flush :: proc(sock: net.TCP_Socket, send_buf: ^cb.Circular_Buffer) -> (ok: bool) {
	for {
		buf := cb.peek_read(send_buf)
		if len(buf) == 0 do break
		n, err := io.send(sock, {buf})
		if err != nil || n <= 0 do return false
		cb.commit_read(send_buf, n)
	}
	return true
}

@(private = "file")
parse_range_header :: proc(
	range_str: string,
	total_size: int,
) -> (
	start: int,
	end: int,
	ok: bool,
) {
	if !strings.has_prefix(range_str, "bytes=") do return 0, total_size, false

	spec := strings.trim_prefix(range_str, "bytes=")

	dash_idx := strings.index_byte(spec, '-')
	if dash_idx == -1 do return 0, 0, false

	start_str := spec[:dash_idx]
	end_str := spec[dash_idx + 1:]

	end = total_size - 1

	if len(start_str) > 0 {
		val, ok := strconv.parse_int(start_str)
		if !ok do return 0, 0, false
		start = val
	}

	if len(end_str) > 0 {
		val, ok := strconv.parse_int(end_str)
		if !ok do return 0, 0, false
		end = val
	} else if len(start_str) == 0 {
		return 0, 0, false
	}

	if len(start_str) == 0 && len(end_str) > 0 {
		start = total_size - end
		end = total_size - 1
	}

	if start < 0 do start = 0
	if end >= total_size do end = total_size - 1
	if start > end do return 0, 0, false

	return start, end, true
}

