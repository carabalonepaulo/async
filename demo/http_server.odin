package main

import "core:fmt"
import "core:os"
import "core:time"

import "../async"
import http "../async/http/server"
import "../async/http/server/headers"
import "../async/http/server/router"
import "../async/io"

running := true

logger :: proc(ctx: ^router.Context) -> (ok: bool) {
	start := time.now()
	router.next(ctx)
	fmt.printfln("[%v] %d %v - %v", ctx.req.method, ctx.res.status, ctx.req.uri, time.since(start))
	return true
}

http_server_demo :: proc() {
	State :: struct {}
	state := State{}

	r: router.Router(State)
	router.init(&r, &state)
	defer router.deinit(&r)

	router.use(&r, logger)

	router.get(&r, "/echo", proc(state: ^State, ctx: ^router.Context) -> (ok: bool) {
		return http.send_text(ctx.res, .Ok, http.get_body_as_text(ctx.req))
	})

	router.get(&r, "/json", proc(state: ^State, ctx: ^router.Context) -> bool {
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

		return http.send_json(ctx.res, result)
	})

	router.get(&r, "/sse", proc(state: ^State, ctx: ^router.Context) -> bool {
		http.begin_sse(ctx.res) or_return
		defer http.end_sse(ctx.res)

		if ctx.req.method == .Head do return true

		for i in 0 ..< 5 {
			msg := fmt.tprintf("message #%d", i)
			id := fmt.tprintf("%d", i)
			http.send_sse(ctx.res, transmute([]u8)(msg), "ping", id) or_return
			async.sleep(time.Second)
		}

		return true
	})

	router.get(&r, "/chunked", proc(state: ^State, ctx: ^router.Context) -> bool {
		headers.add(&ctx.res.headers, "Content-Type", "text/plain; charset=utf-8", .Replace)

		http.begin_chunked(ctx.res, .Ok) or_return
		defer http.end_chunked(ctx.res)

		if ctx.req.method == .Head do return true

		for i in 0 ..< 5 {
			msg := fmt.tprintf("chunk #%d\n", i)
			http.send_chunk(ctx.res, transmute([]u8)(msg))
		}

		return true
	})

	router.get(&r, "/file", proc(state: ^State, ctx: ^router.Context) -> bool {
		file_path := `C:\Users\kelaia\Videos\camera_clamp.mp4`
		return http.send_file(ctx.req, ctx.res, file_path)
	})

	router.get(&r, "/shutdown", proc(state: ^State, ctx: ^router.Context) -> bool {
		running = false
		return false
	})

	router.get(&r, "/", proc(state: ^State, ctx: ^router.Context) -> bool {
		id := os.get_current_thread_id()
		return http.send_text(ctx.res, .Ok, fmt.tprintf("hello, world! / id: %v", id))
	})

	Params :: struct {
		id:   int,
		name: string,
	}

	router.get(
		&r,
		"/hello-{id}/{name}",
		Params,
		proc(state: ^State, ctx: ^router.Context, params: ^Params) -> bool {
			text := fmt.tprintf("hello, %s! id: %v", params.name, params.id)
			return http.send_text(ctx.res, .Ok, text)
		},
	)

	server: http.Server
	http.init(&server, 3000, &r, router.dispatch(&r))
	defer http.deinit(&server)

	for async.get_pending() > 0 {
		if !running do http.close(&server)
		async.poll()
		io.poll()
	}
}

