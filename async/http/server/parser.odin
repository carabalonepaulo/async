package async_http_server

import cb "../../circular_buffer"
import "core:fmt"
import "core:os"
import "core:slice"
import "core:strconv"
import "core:strings"
import "core:testing"

MAX_LINE_SIZE :: 1024

State :: enum {
	Request_Line,
	Headers,
	Body,
}

Parser :: struct {
	state:    State,
	buf:      cb.Circular_Buffer,
	line_buf: []u8,
	body_buf: []u8,
	req:      Request,
}

parser_create :: proc(buf: []u8, line_buf: []u8, body_buf: []u8) -> Parser {
	return Parser{buf = cb.create(buf), line_buf = line_buf, body_buf = body_buf}
}

parser_reset :: proc(self: ^Parser) {
	parser_destroy_request(&self.req)

	self.req.method = ""
	self.req.uri = ""
	self.req.version = ""
	self.req.headers = {}
	self.req.remaining = nil
	self.req.content_length = 0

	self.state = .Request_Line
	cb.clear(&self.buf)
}

parser_destroy_request :: proc(req: ^Request) {
	delete(req.method)
	delete(req.uri)
	delete(req.version)

	for k, v in req.headers {
		delete_key(&req.headers, k)
		delete(k)
		delete(v)
	}
	delete(req.headers)
}

parser_peek_write :: proc(self: ^Parser) -> []u8 {
	return cb.peek_write(&self.buf)
}

parser_commit_write :: proc(self: ^Parser, n: int) {
	cb.commit_write(&self.buf, n)
}

parser_parse :: proc(self: ^Parser) -> (finished: bool, ok: bool) {
	for {
		switch self.state {
		case .Request_Line:
			line, ok := parser_read_line(self)
			if !ok do return false, true

			parts := strings.split(line, " ", context.temp_allocator)
			if len(parts) != 3 do return false, false

			self.req.method = strings.clone(parts[0])
			self.req.uri = strings.clone(parts[1])
			self.req.version = strings.clone(parts[2])

			self.state = .Headers
		case .Headers:
			line, ok := parser_read_line(self)
			if !ok do return false, true

			if line == "" {
				self.req.remaining_bytes = self.req.content_length
				self.state = .Body

				if self.buf.ra > 0 && self.req.content_length > 0 {
					buf_len := min(self.buf.ra, self.req.content_length)
					cb.read(&self.buf, self.body_buf[:buf_len])
					self.req.remaining = self.body_buf[:buf_len]
				} else {
					self.req.remaining = nil
				}

				return true, true
			}

			if idx := strings.index(line, ":"); idx != -1 {
				key := strings.clone(strings.trim_space(line[:idx]))
				val := strings.clone(strings.trim_space(line[idx + 1:]))

				self.req.headers[key] = val

				if strings.equal_fold(key, "Content-Length") {
					val_int, ok := strconv.parse_int(val)
					if !ok do return false, false
					self.req.content_length = val_int
				}
			}
		case .Body:
			return true, true
		}
	}
}

parser_read_line :: proc(self: ^Parser) -> (line: string, ok: bool) {
	SEP :: []u8{'\r', '\n'}

	idx := cb.index_of_bytes(&self.buf, SEP)
	if idx == -1 do return "", false

	dst := self.line_buf[:idx + len(SEP)]
	cb.read(&self.buf, dst) or_return

	return transmute(string)(dst[:idx]), true
}

@(test)
test_read_line :: proc(t: ^testing.T) {
	text := "GET / HTTP/1.1\r\nHeader: value\r\n\r\n"
	back_buf := [1024]u8{}
	line_buf := [256]u8{}
	body_buf := [1024]u8{}

	p := parser_create(back_buf[:], line_buf[:], body_buf[:])
	cb.write(&p.buf, transmute([]u8)(text))
	line, ok := parser_read_line(&p)

	testing.expect(t, ok)
	testing.expect(t, "GET / HTTP/1.1" == line)
}

