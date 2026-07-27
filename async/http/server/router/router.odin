package async_http_server_router

import ".."
import "core:strings"

Middleware :: struct {}

Context :: struct {
	ud:       rawptr,
	req:      ^server.Request,
	res:      ^server.Response,
	//
	handlers: []Handler,
	idx:      int,
}

Handler :: proc(ctx: ^Context)

Route :: struct {
	method:   string,
	path:     string,
	handlers: [dynamic]Handler,
}

Router :: struct {
	handlers: [dynamic]Handler,
	routes:   [dynamic]Route,
	ud:       rawptr,
}

init :: proc(self: ^Router, ud: rawptr = nil) {
	self.ud = ud
	self.handlers = make([dynamic]Handler)
	self.routes = make([dynamic]Route)
}

deinit :: proc(self: ^Router) {
	delete(self.handlers)
	for route in self.routes do delete(route.handlers)
	delete(self.routes)
}

next :: proc(ctx: ^Context) {
	ctx.idx += 1
	if ctx.idx < len(ctx.handlers) {
		ctx.handlers[ctx.idx](ctx)
	}
}

use :: proc(router: ^Router, handler: Handler) {
	append(&router.handlers, handler)
}

dispatch :: proc(ud: rawptr, req: ^server.Request, res: ^server.Response) {
	router := (^Router)(ud)

	target_route: ^Route = nil
	for &route in router.routes {
		if route.method == req.method && route.path == req.uri {
			target_route = &route
			break
		}
	}

	pipeline := make([dynamic]Handler, context.temp_allocator)
	for mw in router.handlers do append(&pipeline, mw)

	if target_route != nil {
		for h in target_route.handlers do append(&pipeline, h)
	} else {
		append(&pipeline, not_found)
	}

	ctx := Context {
		ud       = router.ud,
		req      = req,
		res      = res,
		handlers = pipeline[:],
		idx      = 0,
	}

	if len(ctx.handlers) > 0 do ctx.handlers[0](&ctx)
}

get :: proc(router: ^Router, path: string, handler: Handler, mws: ..Handler) {
	route := Route {
		method = "GET",
		path   = path,
	}

	for mw in mws do append(&route.handlers, mw)
	append(&route.handlers, handler)
	append(&router.routes, route)
}

@(private = "file")
not_found :: proc(ctx: ^Context) {
	ctx.res.status = .Not_Found
	ctx.res.headers["Content-Type"] = "text/plain; charset=utf-8"
	ctx.res.body = transmute([]u8)(strings.clone("404 Not Found", context.temp_allocator))
}

