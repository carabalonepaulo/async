package main

import "core:encoding/json"
import "core:fmt"
import "core:strings"
import "core:sys/windows"
import "core:time"

import "../async"
import http "../async/http/server"
import "../async/http/server/router"
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

logger :: proc(ctx: ^router.Context) {
	start := time.now()
	router.next(ctx)
	fmt.printfln("[%v] %d %v - %v", ctx.req.method, ctx.res.status, ctx.req.uri, time.since(start))
}

http_server_demo :: proc() {
	windows.SetConsoleCtrlHandler(ctrl_handler, true)

	r: router.Router
	router.init(&r)
	defer router.deinit(&r)

	router.use(&r, logger)

	router.get(&r, "/echo", proc(ctx: ^router.Context) {
		ctx.res.headers["Content-Type"] = "text/plain; charset=utf-8"
		ctx.res.body = transmute([]u8)(http.get_body_as_text(ctx.req))
	})

	router.get(&r, "/json", proc(ctx: ^router.Context) {
		Value :: struct {
			name: string,
			age:  int,
		}

		Result :: struct {
			status: string,
			text:   Maybe(string),
		}

		result := Result{"fail", nil}
		value, ok := http.get_body_as_json(ctx.req, Value)
		if ok do result = Result{"success", fmt.tprint(value)}

		buf, _ := json.marshal(result, allocator = context.temp_allocator)
		ctx.res.headers["Content-Type"] = "application/json"
		ctx.res.body = buf
	})

	router.get(&r, "/", proc(ctx: ^router.Context) {
		ctx.res.headers["Content-Type"] = "text/plain; charset=utf-8"
		ctx.res.body = transmute([]u8)(strings.clone("hello world!", context.temp_allocator))
	})

	server: http.Server
	http.init(&server, 3000, &r, router.dispatch)
	defer http.deinit(&server)

	for async.get_pending() > 0 {
		if !running do http.deinit(&server)
		async.poll()
		io.poll()
	}
}

// on_request :: proc(state: ^State, req: ^http.Request, res: ^http.Response) {
// 	fmt.printfln("[request] %v - %v", req.method, req.uri)

// 	sb: strings.Builder
// 	strings.builder_init(&sb)
// 	defer strings.builder_destroy(&sb)

// 	buf: [256]u8

// 	for {
// 		n, _ := http.read(req, buf[:])
// 		if n == 0 do break
// 		fmt.printfln("[chunk:%v] %v", len(buf), buf)

// 		strings.write_bytes(&sb, buf[:n])
// 	}
// 	fmt.printfln("[received] %v", strings.to_string(sb))

// 	res.headers["Content-Type"] = "text/plain; charset=utf-8"
// 	res.body = transmute([]u8)(strings.clone(strings.to_string(sb), context.temp_allocator))
// }

