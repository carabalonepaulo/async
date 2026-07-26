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

File_Path :: distinct string

Response :: struct {
	status:  Status,
	headers: map[string]string,
	body:    union {
		[]u8,
		File_Path,
	},
}

response_reset :: proc(res: ^Response) {
	res.status = .OK
	clear(&res.headers)
	res.body = nil
}

response_send :: proc(server: ^Server, client: ^Client, res: ^Response) -> (ok: bool) {
	buf: [BUFFER_SIZE]u8
	sb := strings.builder_from_slice(buf[:])

	switch body in res.body {
	case []u8:
		body_len := len(body)
		build(&sb, res, i64(body_len))

		bytes_written := strings.builder_len(sb)
		remaining := len(buf) - bytes_written

		if body_len > 0 && body_len <= remaining {
			strings.write_bytes(&sb, body)
			text := strings.to_string(sb)
			bytes := transmute([]u8)(text)
			return try_send_all(client.sock, bytes)
		}

		try_send_builder(client, &sb) or_return
		if body_len > 0 do try_send_all(client.sock, body) or_return
	case File_Path:
		path := (string)(body)
		file, open_err := io.open(path, {.Read})
		if open_err != nil {
			res.status = .Not_Found
			build(&sb, res)
			return try_send_builder(client, &sb)
		}
		defer io.close(file)

		type, size, stat_err := io.stat(file)
		if stat_err != nil || type != .Regular {
			res.status = .Internal_Server_Error
			build(&sb, res)
			return try_send_builder(client, &sb)
		}

		if "Content-Type" not_in res.headers {
			res.headers["Content-Type"] = get_mime_type(&server.mime_types, path)
		}

		build(&sb, res, size)
		try_send_builder(client, &sb) or_return
		send_err := io.send_file(client.sock, file, 0)
		if send_err != nil do return false
	}

	return true
}

@(private = "file")
build :: proc(sb: ^strings.Builder, res: ^Response, size: i64 = 0) {
	fmt.sbprintf(sb, "HTTP/1.1 %d %s\r\n", int(res.status), get_status_text(res.status))
	for k, v in res.headers do fmt.sbprintf(sb, "%s: %s\r\n", k, v)
	fmt.sbprintf(sb, "Content-Length: %d\r\n", size)
	strings.write_string(sb, "Connection: keep-alive\r\n\r\n")
}

@(private = "file")
try_send_builder :: proc(client: ^Client, sb: ^strings.Builder) -> bool {
	text := strings.to_string(sb^)
	bytes := transmute([]u8)(text)
	return try_send_all(client.sock, bytes)
}

@(private = "file")
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

