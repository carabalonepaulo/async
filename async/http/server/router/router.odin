package async_http_server_router_v2

import "base:builtin"
import "core:fmt"
import "core:testing"

import server ".."

Route :: struct {
	method:     server.Method,
	frags:      []Fragment,
	handler:    rawptr,
	handlers:   []Handler,
	trampoline: proc(
		handler_ptr: rawptr,
		state: rawptr,
		ctx: ^Context,
		path: string,
		frags: []Fragment,
		match_ctx: []Param_Range,
	) -> bool,
}

Context :: struct {
	ud:           rawptr,
	req:          ^server.Request,
	res:          ^server.Response,
	handlers:     []Handler,
	idx:          int,
	target_route: ^Route,
	ranges:       []Param_Range,
}

Handler :: proc(ctx: ^Context) -> bool

Router :: struct($S: typeid) {
	state:    ^S,
	handlers: [dynamic]Handler,
	routes:   [dynamic]Route,
	index:    Index,
}

init :: proc(self: ^Router($S), state: ^S) {
	self.state = state
	self.handlers = make([dynamic]Handler)
	self.routes = make([dynamic]Route)
	index_init(&self.index)
}

deinit :: proc(self: ^Router($S)) {
	index_deinit(&self.index)
	builtin.delete(self.handlers)
	for route in self.routes do builtin.delete(route.frags)
	builtin.delete(self.routes)
}

use :: proc(router: ^Router($S), handler: Handler) {
	append(&router.handlers, handler)
}

route :: proc {
	route_static,
	route_dyn,
}

route_static :: proc(
	self: ^Router($S),
	method: server.Method,
	pattern: string,
	handler: proc(state: ^S, ctx: ^Context) -> bool,
	mws: ..Handler,
) {
	Empty :: struct {}
	frags, err := parse_pattern(pattern, Empty)
	if err != .None do fmt.panicf("failed to parse static route '%s': %v", pattern, err)

	handlers := make([]Handler, len(mws))
	copy_slice(handlers, mws)

	r := Route {
		method = method,
		frags = frags,
		handler = auto_cast handler,
		handlers = handlers,
		trampoline = proc(
			handler: rawptr,
			state: rawptr,
			ctx: ^Context,
			_: string,
			_: []Fragment,
			_: []Param_Range,
		) -> bool {
			typed_handler := (proc(state: ^S, ctx: ^Context) -> bool)(handler)
			return typed_handler((^S)(state), ctx)
		},
	}

	index_add(&self.index, len(self.routes), pattern)
	append(&self.routes, r)
}

route_dyn :: proc(
	self: ^Router($S),
	method: server.Method,
	pattern: string,
	$T: typeid,
	handler: proc(state: ^S, ctx: ^Context, params: ^T) -> bool,
	mws: ..Handler,
) {
	frags, err := parse_pattern(pattern, T)
	if err != .None do fmt.panicf("failed to parse route '%s': %v", pattern, err)

	handlers := make([]Handler, len(mws))
	copy_slice(handlers, mws)

	r := Route {
		method = method,
		frags = frags,
		handler = auto_cast handler,
		handlers = handlers,
		trampoline = proc(
			handler: rawptr,
			state: rawptr,
			ctx: ^Context,
			path: string,
			frags: []Fragment,
			ranges: []Param_Range,
		) -> bool {
			params: T
			if !deserialize_params(path, frags, ranges, &params) do return invalid_params(ctx)

			typed_handler := (proc(state: ^S, ctx: ^Context, params: ^T) -> bool)(handler)
			return typed_handler((^S)(state), ctx, &params)
		},
	}

	index_add(&self.index, len(self.routes), pattern)
	append(&self.routes, r)
}

dispatch :: proc(self: ^Router($S)) -> server.Request_Handler {
	return proc(ud: rawptr, req: ^server.Request, res: ^server.Response) -> bool {
			self := (^Router(S))(ud)
			return _dispatch(self, req, res)
		}
}

@(private = "file")
find_route :: proc(
	self: ^Router($S),
	req: ^server.Request,
	ranges: ^[dynamic]Param_Range,
) -> ^Route {
	it: Index_Iterator
	index_iter_init(&it, req.uri)

	for route_idx in index_iter(&self.index, &it) {
		route := &self.routes[route_idx]

		if !(route.method == req.method || route.method == .Get && req.method == .Head) do continue
		if match_pattern(req.uri, route.frags, ranges) do return route
	}

	return nil
}

@(private = "file")
_dispatch :: proc(self: ^Router($S), req: ^server.Request, res: ^server.Response) -> bool {
	ranges := make([dynamic]Param_Range, 0, 4)
	defer builtin.delete(ranges)

	handlers := make([dynamic]Handler, len(self.handlers))
	defer builtin.delete(handlers)
	copy_slice(handlers[:], self.handlers[:])

	target_route := find_route(self, req, &ranges)
	if target_route != nil {
		append_elems(&handlers, ..target_route.handlers)
		append(&handlers, endpoint_handler)
	} else do append(&handlers, not_found)

	ctx := Context {
		ud           = self.state,
		req          = req,
		res          = res,
		handlers     = handlers[:],
		idx          = 0,
		target_route = target_route,
		ranges       = ranges[:],
	}

	return next(&ctx)
}

next :: proc(ctx: ^Context) -> bool {
	if ctx.idx < len(ctx.handlers) {
		handler := ctx.handlers[ctx.idx]
		ctx.idx += 1
		return handler(ctx)
	}
	return true
}

@(private = "file")
endpoint_handler :: proc(ctx: ^Context) -> bool {
	if ctx.target_route == nil do return not_found(ctx)

	r := ctx.target_route
	return r.trampoline(r.handler, ctx.ud, ctx, ctx.req.uri, r.frags, ctx.ranges)
}

@(private = "file")
not_found :: proc(ctx: ^Context) -> bool {
	return server.send_text(ctx.res, .Not_Found, "404 Not Found")
}

@(private = "file")
invalid_params :: proc(ctx: ^Context) -> bool {
	return server.send_text(ctx.res, .Internal_Server_Error, "Invalid Params")
}

@(test)
test_defining_route :: proc(t: ^testing.T) {
	State :: struct {
		t: ^testing.T,
	}

	state := State{t}

	r: Router(State)
	init(&r, &state)
	defer deinit(&r)

	Hello_Params :: struct {
		name: string,
	}

	HELLO :: proc(state: ^State, ctx: ^Context, params: ^Hello_Params) -> bool {
		return false
	}
	route(&r, .Get, "/hello-{name}", Hello_Params, HELLO)
}

