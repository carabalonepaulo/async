package async_http_server

import "core:strconv"
import "core:strings"
import "core:testing"

import cb "../../circular_buffer"
import "headers"

State :: enum {
	Request_Line,
	Headers,
	Body,
}

Parser :: struct {
	state:           State,
	buf:             cb.Circular_Buffer,
	line_buf:        []u8,
	req:             Request,
	remaining:       []u8,
	remaining_bytes: int,
}

parser_init :: proc(self: ^Parser, buf: []u8, line_buf: []u8) {
	self.buf = cb.create(buf)
	self.line_buf = line_buf
	self.req = Request {
		internal = self,
	}
}

parser_reset :: proc(self: ^Parser) {
	parser_destroy_request(&self.req)

	self.req.method = .Get
	self.req.uri = ""
	self.req.version = ""
	self.req.headers = nil
	self.req.content_length = 0
	self.req.internal = self

	self.state = .Request_Line
	cb.clear(&self.buf)
}

parser_destroy_request :: proc(req: ^Request) {
	self := (^Parser)(req.internal)
	if self.state == .Request_Line do return

	delete(req.uri)
	delete(req.version)

	for header in req.headers {
		delete(header.key)
		delete(header.value)
	}
	delete(req.headers)
}

parser_peek_write :: proc(self: ^Parser) -> []u8 {
	return cb.peek_write(&self.buf)
}

parser_commit_write :: proc(self: ^Parser, n: int) {
	cb.commit_write(&self.buf, n)
}

Parse_Result :: enum {
	Partial,
	Done,
	//
	Invalid_Method,
	Invalid_HTTP_Version,
	Invalid_Request_Line,
	Invalid_Content_Length,
}

parser_parse :: proc(self: ^Parser) -> Parse_Result {
	for {
		switch self.state {
		case .Request_Line:
			line, ok := parser_read_line(self)
			if !ok do return .Partial

			parts := strings.split(line, " ", context.temp_allocator)
			if len(parts) != 3 do return .Invalid_Request_Line

			method, method_ok := parse_method(parts[0])
			if !method_ok do return .Invalid_Method

			self.req.method = method
			self.req.uri = strings.clone(parts[1])
			self.req.version = strings.clone(parts[2])

			self.state = .Headers
		case .Headers:
			line, ok := parser_read_line(self)
			if !ok do return .Partial

			if line == "" {
				self.remaining_bytes = self.req.content_length
				self.state = .Body
				return .Done
			}

			if idx := strings.index(line, ":"); idx != -1 {
				key := strings.clone(strings.trim_space(line[:idx]))
				val := strings.clone(strings.trim_space(line[idx + 1:]))

				headers.add(&self.req.headers, key, val)

				if strings.equal_fold(key, "Content-Length") {
					val_int, ok := strconv.parse_int(val)
					if !ok do return .Invalid_Content_Length
					self.req.content_length = val_int
				}
			}
		case .Body:
			return .Done
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

@(private)
parse_method :: proc(method: string) -> (Method, bool) {
	switch strings.to_lower(method, allocator = context.temp_allocator) {
	case "get":
		return .Get, true
	case "head":
		return .Head, true
	case "post":
		return .Post, true
	case "delete":
		return .Delete, true
	case "connect":
		return .Connect, true
	case "options":
		return .Options, true
	case "trace":
		return .Trace, true
	case "patch":
		return .Patch, true
	case:
		return .Get, false
	}
}

@(test)
test_read_line :: proc(t: ^testing.T) {
	text := "GET / HTTP/1.1\r\nHeader: value\r\n\r\n"
	back_buf := [1024]u8{}
	line_buf := [256]u8{}

	p: Parser
	parser_init(&p, back_buf[:], line_buf[:])
	cb.write(&p.buf, transmute([]u8)(text))
	line, ok := parser_read_line(&p)

	testing.expect(t, ok)
	testing.expect(t, "GET / HTTP/1.1" == line)
}

