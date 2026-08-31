package async_http_server_router

import ".."

Middleware :: struct {}

Context :: struct {
	ud:       rawptr,
	req:      ^server.Request,
	res:      ^server.Response,
	//
	handlers: []Handler,
	idx:      int,
}

Handler :: proc(ctx: ^Context) -> bool

Route :: struct {
	method:   server.Method,
	// method:   string,
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

dispatch :: proc(ud: rawptr, req: ^server.Request, res: ^server.Response) -> bool {
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

	if len(ctx.handlers) > 0 {
		return ctx.handlers[0](&ctx)
	}

	return true
}

post :: proc(router: ^Router, path: string, handler: Handler, mws: ..Handler) {
	route := Route {
		method = .Post,
		path   = path,
	}

	for mw in mws do append(&route.handlers, mw)
	append(&route.handlers, handler)
	append(&router.routes, route)
}

get :: proc(router: ^Router, path: string, handler: Handler, mws: ..Handler) {
	route := Route {
		method = .Get,
		path   = path,
	}

	for mw in mws do append(&route.handlers, mw)
	append(&route.handlers, handler)
	append(&router.routes, route)
}

@(private = "file")
not_found :: proc(ctx: ^Context) -> bool {
	return server.send_text(ctx.res, .Not_Found, "404 Not Found")
}

