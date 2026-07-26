package main

import "core:strings"
import "core:sys/windows"

import "../async"
import http "../async/http/server"
import "../async/io"

running := true

State :: struct {}

ctrl_handler :: proc "stdcall" (ctrl_type: windows.DWORD) -> windows.BOOL {
	switch ctrl_type {
	case windows.CTRL_C_EVENT, windows.CTRL_BREAK_EVENT, windows.CTRL_CLOSE_EVENT:
		running = false
		return true
	}
	return false
}

http_server_demo :: proc() {
	windows.SetConsoleCtrlHandler(ctrl_handler, true)

	state := new(State)

	server: http.Server
	http.init(&server, 3000, state, auto_cast on_request)
	defer http.deinit(&server)

	for async.get_pending() > 0 {
		if !running do http.deinit(&server)
		async.poll()
		io.poll()
	}
}

on_request :: proc(state: ^State, req: ^http.Request, res: ^http.Response) {
	res.headers["Content-Type"] = "text/plain"
	res.body = transmute([]u8)(strings.clone("hello world!", context.temp_allocator))
}

