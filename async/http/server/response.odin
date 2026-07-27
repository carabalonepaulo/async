package async_http_server

import "core:fmt"
import "core:net"
import "core:strconv"
import "core:strings"

import "../../io"

Status :: enum {
	// 1xx Informational
	Continue                      = 100,
	Switching_Protocols           = 101,

	// 2xx Success
	Ok                            = 200,
	Created                       = 201,
	Accepted                      = 202,
	Non_Authoritative_Information = 203,
	No_Content                    = 204,
	Reset_Content                 = 205,
	Partial_Content               = 206,

	// 3xx Redirection
	Multiple_Choices              = 300,
	Moved_Permanently             = 301,
	Found                         = 302,
	See_Other                     = 303,
	Not_Modified                  = 304,
	Temporary_Redirect            = 307,
	Permanent_Redirect            = 308,

	// 4xx Client Errors
	Bad_Request                   = 400,
	Unauthorized                  = 401,
	Payment_Required              = 402,
	Forbidden                     = 403,
	Not_Found                     = 404,
	Method_Not_Allowed            = 405,
	Not_Acceptable                = 406,
	Request_Timeout               = 408,
	Conflict                      = 409,
	Gone                          = 410,
	Length_Required               = 411,
	Payload_Too_Large             = 413,
	URI_Too_Long                  = 414,
	Unsupported_Media_Type        = 415,
	Range_Not_Satisfiable         = 416,
	Unprocessable_Entity          = 422,
	Too_Many_Requests             = 429,

	// 5xx Server Errors
	Internal_Server_Error         = 500,
	Not_Implemented               = 501,
	Bad_Gateway                   = 502,
	Service_Unavailable           = 503,
	Gateway_Timeout               = 504,
	HTTP_Version_Not_Supported    = 505,
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

@(private)
response_reset :: proc(res: ^Response) {
	res.status = .Ok
	clear(&res.headers)
	res.body = nil
}

@(private)
response_send :: proc(
	server: ^Server,
	client: ^Client,
	req: ^Request,
	res: ^Response,
) -> (
	ok: bool,
) {
	buf: [BUFFER_SIZE]u8
	sb := strings.builder_from_slice(buf[:])

	switch body in res.body {
	case []u8:
		body_len := len(body)
		build(&sb, res, body_len)

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
			res.headers["Content-Type"] = get_mime_type_from_path(&server.mime_types, path)
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
				build(&sb, res, 0)
				return try_send_builder(client, &sb)
			}
		}

		build(&sb, res, length)
		try_send_builder(client, &sb) or_return
		send_err := io.send_file(client.sock, file, offset, length)
		if send_err != nil do return false
	}

	return true
}

@(private = "file")
build :: proc(sb: ^strings.Builder, res: ^Response, size: int = 0) {
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
	// 1xx
	case .Continue:
		return "Continue"
	case .Switching_Protocols:
		return "Switching Protocols"

	// 2xx
	case .Ok:
		return "Ok"
	case .Created:
		return "Created"
	case .Accepted:
		return "Accepted"
	case .Non_Authoritative_Information:
		return "Non-Authoritative Information"
	case .No_Content:
		return "No Content"
	case .Reset_Content:
		return "Reset Content"
	case .Partial_Content:
		return "Partial Content"

	// 3xx
	case .Multiple_Choices:
		return "Multiple Choices"
	case .Moved_Permanently:
		return "Moved Permanently"
	case .Found:
		return "Found"
	case .See_Other:
		return "See Other"
	case .Not_Modified:
		return "Not Modified"
	case .Temporary_Redirect:
		return "Temporary Redirect"
	case .Permanent_Redirect:
		return "Permanent Redirect"

	// 4xx
	case .Bad_Request:
		return "Bad Request"
	case .Unauthorized:
		return "Unauthorized"
	case .Payment_Required:
		return "Payment Required"
	case .Forbidden:
		return "Forbidden"
	case .Not_Found:
		return "Not Found"
	case .Method_Not_Allowed:
		return "Method Not Allowed"
	case .Not_Acceptable:
		return "Not Acceptable"
	case .Request_Timeout:
		return "Request Timeout"
	case .Conflict:
		return "Conflict"
	case .Gone:
		return "Gone"
	case .Length_Required:
		return "Length Required"
	case .Payload_Too_Large:
		return "Payload Too Large"
	case .URI_Too_Long:
		return "URI Too Long"
	case .Unsupported_Media_Type:
		return "Unsupported Media Type"
	case .Range_Not_Satisfiable:
		return "Range Not Satisfiable"
	case .Unprocessable_Entity:
		return "Unprocessable Entity"
	case .Too_Many_Requests:
		return "Too Many Requests"

	// 5xx
	case .Internal_Server_Error:
		return "Internal Server Error"
	case .Not_Implemented:
		return "Not Implemented"
	case .Bad_Gateway:
		return "Bad Gateway"
	case .Service_Unavailable:
		return "Service Unavailable"
	case .Gateway_Timeout:
		return "Gateway Timeout"
	case .HTTP_Version_Not_Supported:
		return "HTTP Version Not Supported"
	}

	return "Unknown"
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

