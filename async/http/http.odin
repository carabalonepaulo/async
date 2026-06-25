package http

import "base:runtime"
import "core:c"
import "core:fmt"
import "core:mem"
import "core:strings"
import "vendor:curl"

import async ".."

CA_PEM :: #load("cacert.pem")

Error :: enum {
	None,
	Easy_Init_Failed,
	Multi_Init_Failed,
	Perform_Failed,
}

Method :: enum {
	Get,
	Post,
}

Http_Callback :: #type proc(resp: ^Response, err: Error, user_data: rawptr)

Request_Task :: struct {
	resp:    ^Response,
	h_state: ^Header_State,
	w_state: ^Write_State,
	slist:   ^curl.slist,
	c_url:   cstring,
	handle:  async.Handle,
}

Client :: struct {
	allocator:       mem.Allocator,
	multi:           ^curl.CURLM,
	active_requests: map[^curl.CURL]Request_Task,
}

Header :: struct {
	key, value: string,
}

Response :: struct {
	status:    i32,
	headers:   [dynamic]Header,
	body:      [dynamic]u8,
	allocator: mem.Allocator,
}

Header_State :: struct {
	headers:   ^[dynamic]Header,
	allocator: mem.Allocator,
}

Write_State :: struct {
	body:      ^[dynamic]u8,
	allocator: mem.Allocator,
}

init :: proc(self: ^Client, allocator := context.allocator) -> Error {
	self.allocator = allocator

	self.multi = curl.multi_init()
	if self.multi == nil {
		return .Multi_Init_Failed
	}

	self.active_requests = make(map[^curl.CURL]Request_Task, 16, allocator)

	return .None
}

deinit :: proc(self: ^Client) {
	if self.multi != nil {
		curl.multi_cleanup(self.multi)
	}
	delete(self.active_requests)
}

get :: proc(self: ^Client, url: string) -> (^Response, Error) {
	return fetch(self, .Get, url, nil, nil)
}

fetch :: proc(
	self: ^Client,
	method: Method,
	url: string,
	headers: map[string]string,
	body: []u8,
) -> (
	resp: ^Response,
	err: Error,
) {
	easy_handle := curl.easy_init()
	if easy_handle == nil {
		return nil, .Easy_Init_Failed
	}

	curl.easy_setopt(
		easy_handle,
		.CAINFO_BLOB,
		curl.blob{data = raw_data(CA_PEM), len = len(CA_PEM), flags = curl.blob_flags{.COPY}},
	)
	curl.easy_setopt(easy_handle, .ACCEPT_ENCODING, cstring(""))

	c_url := strings.clone_to_cstring(url, self.allocator)
	curl.easy_setopt(easy_handle, .URL, c_url)

	slist: ^curl.slist = nil
	if headers != nil {
		for key, value in headers {
			h_str := fmt.aprintf("%s: %s", key, value, self.allocator)
			slist = curl.slist_append(slist, strings.clone_to_cstring(h_str, self.allocator))
		}
		curl.easy_setopt(easy_handle, .HTTPHEADER, slist)
	}

	switch method {
	case .Get:
		curl.easy_setopt(easy_handle, .HTTPGET, i32(1))
	case .Post:
		curl.easy_setopt(easy_handle, .POST, i32(1))
		if len(body) > 0 {
			curl.easy_setopt(easy_handle, .POSTFIELDS, raw_data(body))
			curl.easy_setopt(easy_handle, .POSTFIELDSIZE, curl.off_t(len(body)))
		}
	}

	resp = new(Response, self.allocator)
	resp.allocator = self.allocator
	resp.body = make([dynamic]u8, self.allocator)
	resp.headers = make([dynamic]Header, self.allocator)

	header_state := new(Header_State, self.allocator)
	header_state.headers = &resp.headers
	header_state.allocator = self.allocator
	curl.easy_setopt(easy_handle, .HEADERFUNCTION, header_callback)
	curl.easy_setopt(easy_handle, .HEADERDATA, header_state)

	write_state := new(Write_State, self.allocator)
	write_state.body = &resp.body
	write_state.allocator = self.allocator
	curl.easy_setopt(easy_handle, .WRITEFUNCTION, write_callback)
	curl.easy_setopt(easy_handle, .WRITEDATA, write_state)

	m_err := curl.multi_add_handle(self.multi, easy_handle)
	if m_err != .OK {
		free(header_state, self.allocator)
		free(write_state, self.allocator)
		delete(c_url, self.allocator)
		destroy(resp)
		curl.easy_cleanup(easy_handle)
		return nil, .Perform_Failed
	}

	self.active_requests[easy_handle] = Request_Task {
		resp    = resp,
		h_state = header_state,
		w_state = write_state,
		slist   = slist,
		c_url   = c_url,
		handle  = async.get_handle(),
	}

	final_err := async.recv(Error)
	if final_err != .None do return nil, final_err
	return resp, .None
}

poll :: proc(self: ^Client) {
	if self.multi == nil do return
	if len(self.active_requests) == 0 do return

	still_running: i32 = 0
	m_err := curl.multi_perform(self.multi, &still_running)
	if m_err != .OK do return

	msgs_in_queue: i32 = 0
	msg := curl.multi_info_read(self.multi, &msgs_in_queue)

	for msg != nil {
		if msg.msg == .DONE {
			easy_handle := msg.easy_handle

			task, exists := self.active_requests[easy_handle]
			if exists {
				err := Error.None
				if msg.data.result != .E_OK {
					err = .Perform_Failed
				} else {
					curl.easy_getinfo(easy_handle, .RESPONSE_CODE, &task.resp.status)
				}

				async.send(task.handle, err)

				curl.multi_remove_handle(self.multi, easy_handle)
				curl.easy_cleanup(easy_handle)

				if task.c_url != nil do delete(task.c_url, self.allocator)
				if task.slist != nil do curl.slist_free_all(task.slist)

				free(task.h_state, self.allocator)
				free(task.w_state, self.allocator)

				delete_key(&self.active_requests, easy_handle)
			}
		}

		msg = curl.multi_info_read(self.multi, &msgs_in_queue)
	}
}

destroy :: proc(self: ^Response) {
	if self == nil do return
	context.allocator = self.allocator
	if self.headers != nil {
		for header in self.headers {
			delete(header.key)
			delete(header.value)
		}
		delete(self.headers)
	}
	if self.body != nil {
		delete(self.body)
	}
	free(self)
}

@(private)
header_callback :: proc "c" (
	ptr: rawptr,
	size: c.size_t,
	nmemb: c.size_t,
	ud: rawptr,
) -> c.size_t {
	total_size := size * nmemb
	state := (^Header_State)(ud)

	context = runtime.default_context()
	context.allocator = state.allocator

	line := string(([^]u8)(ptr)[:total_size])

	idx := strings.index_byte(line, ':')
	if idx != -1 {
		key := strings.to_lower(strings.trim_space(line[:idx]))
		val := strings.trim_space(line[idx + 1:])
		cloned_val := strings.clone(val)
		append(state.headers, Header{key, cloned_val})
	}

	return total_size
}

@(private)
write_callback :: proc "c" (ptr: rawptr, size: c.size_t, nmemb: c.size_t, ud: rawptr) -> c.size_t {
	total_size := size * nmemb
	state := (^Write_State)(ud)

	context = runtime.default_context()
	context.allocator = state.allocator

	bytes := ([^]u8)(ptr)[:total_size]
	append(state.body, ..bytes)

	return total_size
}

