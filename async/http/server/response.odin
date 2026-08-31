package async_http_server

import "core:bytes"
import "core:encoding/json"
import "core:fmt"
import "core:nbio"
import "core:strconv"
import "core:strings"

import cb "../../circular_buffer"
import "../../io"
import "headers"

@(private)
Response_Internal :: struct {
	sock:       nbio.TCP_Socket,
	send_buf:   cb.Circular_Buffer,
	line_buf:   []u8,
	mime_types: ^map[string]string,
	method:     Method,
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
	_send_buf(internal, transmute([]u8)(line)) or_return

	res.headers["Connection"] = "keep-alive"

	for k, v in res.headers {
		line = fmt.bprintf(line_buf, "%s: %s\r\n", k, v)
		_send_buf(internal, transmute([]u8)(line)) or_return
	}

	_send_buf(internal, {'\r', '\n'}) or_return
	return flush(internal)
}

send :: proc(res: ^Response, buf: []u8) -> (ok: bool) {
	internal := (^Response_Internal)(res.internal)
	send_buf := &internal.send_buf

	res.headers["Content-Length"] = fmt.tprint(len(buf))
	send_headers(res) or_return

	if internal.method != .Head do _send_buf(internal, buf) or_return
	return flush(internal)
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

	if range_header, has_range := headers.get(&req.headers, "Range"); has_range {
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

	if internal.method != .Head {
		cb :: proc(op: ^nbio.Operation) {nbio.close(op.sendfile.file)}
		nbio.sendfile(internal.sock, file, cb, offset, length)
	}

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
	return send_headers(res)
}

send_chunk :: proc(res: ^Response, buf: []u8) -> (ok: bool) {
	if len(buf) == 0 do return true

	internal := (^Response_Internal)(res.internal)
	line_buf := internal.line_buf
	if internal.method == .Head do return true

	_send_fmt(internal, count_hex_digits(len(buf)) + 2, "%x\r\n", len(buf)) or_return
	_send_buf(internal, buf) or_return
	_send_buf(internal, {'\r', '\n'}) or_return

	return flush(internal)
}

end_chunked :: proc(res: ^Response) -> (ok: bool) {
	internal := (^Response_Internal)(res.internal)
	send_buf := &internal.send_buf
	if internal.method == .Head do return true
	_send_buf(internal, {'0', '\r', '\n', '\r', '\n'}) or_return
	return flush(internal)
}

begin_sse :: proc(res: ^Response) -> (ok: bool) {
	res.headers["Content-Type"] = "text/event-stream"
	res.headers["Cache-Control"] = "no-cache"
	res.headers["Connection"] = "keep-alive"
	return begin_chunked(res, .Ok)
}

send_sse :: proc(
	res: ^Response,
	data: []u8,
	event: string = "",
	id: string = "",
	retry: int = 0,
) -> (
	ok: bool,
) {
	internal := (^Response_Internal)(res.internal)
	send_buf := &internal.send_buf
	line_buf := internal.line_buf

	if internal.method == .Head do return true

	payload_len := 0
	retry_digits := count_decimal_digits(retry)

	if retry > 0 do payload_len += 7 + retry_digits + 1
	if len(id) > 0 do payload_len += 4 + len(id) + 1
	if len(event) > 0 do payload_len += 7 + len(event) + 1

	if len(data) > 0 {
		remaining := data
		for len(remaining) > 0 {
			payload_len += 6
			idx := bytes.index_byte(remaining, '\n')
			if idx != -1 {
				payload_len += idx + 1
				remaining = remaining[idx + 1:]
			} else {
				payload_len += len(remaining) + 1
				break
			}
		}
	}

	payload_len += 1
	if payload_len == 1 do return true
	_send_fmt(internal, count_hex_digits(payload_len) + 2, "%x\r\n", payload_len) or_return

	if retry > 0 do _send_fmt(internal, len("retry: \n") + retry_digits, "retry: %d\n", retry) or_return
	if len(id) > 0 do _send_fmt(internal, len("id: \n") + len(id), "id: %s\n", id) or_return
	if len(event) > 0 do _send_fmt(internal, len("event: \n") + len(event), "event: %s\n", event) or_return

	if len(data) > 0 {
		remaining := data
		for len(remaining) > 0 {
			_send_str(internal, "data: ") or_return

			idx := bytes.index_byte(remaining, '\n')
			if idx != -1 {
				_send_buf(internal, remaining[:idx + 1]) or_return
				remaining = remaining[idx + 1:]
			} else {
				_send_buf(internal, remaining) or_return
				_send_str(internal, "\n") or_return
				break
			}
		}
	}

	_send_str(internal, "\n\r\n") or_return
	return flush(internal)
}

end_sse :: end_chunked

@(private)
_send_fmt :: proc(
	internal: ^Response_Internal,
	estimated_size: int,
	pat: string,
	args: ..any,
) -> (
	ok: bool,
) {
	write_buf := cb.peek_write(&internal.send_buf)

	if len(write_buf) < estimated_size {
		flush(internal) or_return
		if cb.is_empty(&internal.send_buf) do cb.clear(&internal.send_buf)
		write_buf = cb.peek_write(&internal.send_buf)

		when LINE_BUFFER_SIZE <= RESPONSE_BUFFER_SIZE {
			if len(write_buf) < estimated_size do return false
		} else {
			if len(write_buf) < estimated_size && len(internal.line_buf) >= estimated_size {
				line := fmt.bprintf(internal.line_buf, pat, ..args)
				return _send_str(internal, line)
			} else do return false
		}
	}

	fmtd := fmt.bprintf(write_buf, pat, ..args)
	cb.commit_write(&internal.send_buf, len(fmtd))

	return true
}

@(private)
_send_str :: #force_inline proc(internal: ^Response_Internal, text: string) -> (ok: bool) {
	return _send_buf(internal, transmute([]u8)(text))
}

@(private)
_send_buf :: proc(internal: ^Response_Internal, buf: []u8) -> (ok: bool) {
	remaining := buf
	for len(remaining) > 0 {
		send_len := min(len(remaining), internal.send_buf.wa)
		if send_len > 0 {
			cb.write(&internal.send_buf, remaining[:send_len])
			remaining = remaining[send_len:]
		}
		if len(remaining) > 0 do flush(internal) or_return
	}
	return true
}

@(private)
flush :: proc(internal: ^Response_Internal) -> (ok: bool) {
	for {
		buf := cb.peek_read(&internal.send_buf)
		if len(buf) == 0 do break
		n, err := io.send(internal.sock, {buf})
		if err != nil || n <= 0 do return false
		cb.commit_read(&internal.send_buf, n)
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

@(private = "file")
count_decimal_digits :: #force_inline proc(n: int) -> int {
	val := abs(n)
	if n == 0 do return 1
	count := 0
	for val > 0 {
		val /= 10
		count += 1
	}
	return count
}

@(private = "file")
count_hex_digits :: #force_inline proc(n: int) -> int {
	val := abs(n)
	if n == 0 do return 1
	count := 0
	for val > 0 {
		val >>= 4
		count += 1
	}
	return count
}

