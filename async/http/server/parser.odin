package async_http_server

import "core:strconv"
import "core:strings"

Parse_State :: enum {
	Request_Line,
	Headers,
	Complete,
}

Parser :: struct {
	state:        Parse_State,
	buf:          []u8,
	read_cursor:  int,
	write_cursor: int,
	req:          Request,
}

parser_init :: proc(self: ^Parser) {
	self.buf = make([]u8, BUFFER_SIZE)
	self.req.headers = make(map[string]string)
	self.state = .Request_Line
}

parser_deinit :: proc(self: ^Parser) {
	delete(self.buf)
	delete(self.req.headers)
}

parser_reset :: proc(self: ^Parser) {
	self.state = .Request_Line
	clear(&self.req.headers)
	self.req.method = ""
	self.req.uri = ""
	self.req.version = ""
	self.req.body = nil
	self.req.content_length = 0

	parser_compact(self)
}

parser_get_write_slice :: proc(self: ^Parser) -> []u8 {
	if self.write_cursor == len(self.buf) && self.read_cursor > 0 {
		parser_compact(self)
	}
	return self.buf[self.write_cursor:]
}

parser_commit_write :: proc(self: ^Parser, n: int) {
	self.write_cursor += n
}

parser_compact :: proc(self: ^Parser) {
	unparsed_len := self.write_cursor - self.read_cursor
	if unparsed_len > 0 {
		copy(self.buf[:unparsed_len], self.buf[self.read_cursor:self.write_cursor])
		self.write_cursor = unparsed_len
	} else {
		self.write_cursor = 0
	}
	self.read_cursor = 0
}

parser_parse :: proc(self: ^Parser) -> (completed: bool, err: bool) {
	for {
		#partial switch self.state {
		case .Request_Line:
			line_buf, found := read_crlf_line(self)
			if !found do return false, false

			line := transmute(string)(line_buf)
			parts := strings.split(line, " ", context.temp_allocator)
			if len(parts) != 3 do return false, true

			self.req.method = parts[0]
			self.req.uri = parts[1]
			self.req.version = parts[2]

			self.state = .Headers
		case .Headers:
			line_buf, found := read_crlf_line(self)
			if !found do return false, false

			line := transmute(string)(line_buf)
			if line == "" {
				self.req.remaining_bytes = self.req.content_length

				unparsed_len := self.write_cursor - self.read_cursor
				if unparsed_len > 0 && self.req.content_length > 0 {
					body_bytes_in_buf := min(unparsed_len, self.req.content_length)
					self.req.body = self.buf[self.read_cursor:self.read_cursor + body_bytes_in_buf]
					self.read_cursor += body_bytes_in_buf
				} else {
					self.req.body = nil
				}

				self.state = .Complete
				return true, false
			}

			if idx := strings.index(line, ":"); idx != -1 {
				key := strings.trim_space(line[:idx])
				val := strings.trim_space(line[idx + 1:])

				self.req.headers[key] = val

				if strings.equal_fold(key, "Content-Length") {
					val_int, ok := strconv.parse_int(val)
					if !ok do return false, true
					self.req.content_length = val_int
				}
			}
		case .Complete:
			return true, false
		}
	}
}

@(private = "file")
read_crlf_line :: proc(self: ^Parser) -> (line: []u8, found: bool) {
	data := self.buf[self.read_cursor:self.write_cursor]

	for i in 0 ..< len(data) - 1 {
		if data[i] == '\r' && data[i + 1] == '\n' {
			line = data[:i]
			self.read_cursor += i + 2
			return line, true
		}
	}

	return nil, false
}

