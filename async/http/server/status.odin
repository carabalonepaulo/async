package async_http_server

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
	Header_Fields_Too_Large       = 431,

	// 5xx Server Errors
	Internal_Server_Error         = 500,
	Not_Implemented               = 501,
	Bad_Gateway                   = 502,
	Service_Unavailable           = 503,
	Gateway_Timeout               = 504,
	HTTP_Version_Not_Supported    = 505,
}

@(private)
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
	case .Header_Fields_Too_Large:
		return "Header Fields Too Large"

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

