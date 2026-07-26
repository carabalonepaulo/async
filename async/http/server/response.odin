package async_http_server

import "core:fmt"
import "core:net"
import "core:strings"

import "../../io"

Status :: enum {
	OK                    = 200,
	Created               = 201,
	Bad_Request           = 400,
	Not_Found             = 404,
	Internal_Server_Error = 500,
}

Response :: struct {
	status:  Status,
	headers: map[string]string,
	body:    []u8,
}

response_reset :: proc(res: ^Response) {
	res.status = .OK
	clear(&res.headers)
	res.body = nil
}

response_send :: proc(client: ^Client, res: ^Response) -> (ok: bool) {
	buf: [BUFFER_SIZE]u8
	sb := strings.builder_from_slice(buf[:])

	fmt.sbprintf(&sb, "HTTP/1.1 %d %s\r\n", int(res.status), get_status_text(res.status))
	for k, v in res.headers do fmt.sbprintf(&sb, "%s: %s\r\n", k, v)

	body_len := len(res.body)
	fmt.sbprintf(&sb, "Content-Length: %d\r\n", body_len)
	strings.write_string(&sb, "Connection: keep-alive\r\n\r\n")

	bytes_written := strings.builder_len(sb)
	remaining := len(buf) - bytes_written

	if body_len > 0 && body_len <= remaining {
		strings.write_bytes(&sb, res.body)
		text := strings.to_string(sb)
		bytes := transmute([]u8)(text)
		return try_send_all(client.sock, bytes)
	}

	text := strings.to_string(sb)
	bytes := transmute([]u8)(text)
	try_send_all(client.sock, bytes) or_return

	if body_len > 0 do try_send_all(client.sock, res.body) or_return

	return true
}

try_send_all :: proc(sock: net.TCP_Socket, buf: []u8) -> bool {
	buf := buf
	for len(buf) > 0 {
		n, err := io.send(sock, {buf})
		if err != nil || n <= 0 do return false
		buf = buf[n:]
	}
	return true
}

@(private = "file")
get_status_text :: proc(status: Status) -> string {
	switch status {
	case .OK:
		return "OK"
	case .Created:
		return "Created"
	case .Bad_Request:
		return "Bad Request"
	case .Not_Found:
		return "Not Found"
	case .Internal_Server_Error:
		return "Internal Server Error"
	}
	return "Unknown"
}

