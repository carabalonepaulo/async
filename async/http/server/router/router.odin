package async_http_server_router_v2

import "base:builtin"
import "core:fmt"
import "core:testing"

import server ".."

Route :: struct($S: typeid) {
	method:     server.Method,
	frags:      []Fragment,
	handler:    rawptr,
	handlers:   []proc(ctx: ^Context(S)) -> bool,
	trampoline: proc(
		handler_ptr: rawptr,
		ctx: ^Context(S),
		path: string,
		frags: []Fragment,
		match_ctx: []Param_Range,
	) -> bool,
}

Dispatch_State :: struct($S: typeid) {
	router:       ^Router(S),
	handlers:     [dynamic]proc(ctx: ^Context(S)) -> bool,
	idx:          int,
	target_route: ^Route(S),
	ranges:       [dynamic]Param_Range,
}

Context :: struct($S: typeid) {
	state:     ^S,
	req:       ^server.Request,
	res:       ^server.Response,
	_internal: rawptr,
}

Router :: struct($S: typeid) {
	state:    ^S,
	handlers: [dynamic]proc(ctx: ^Context(S)) -> bool,
	routes:   [dynamic]Route(S),
	index:    Index,
}

init :: proc(self: ^Router($S), state: ^S) {
	self.state = state
	self.handlers = make([dynamic]proc(ctx: ^Context(S)) -> bool)
	self.routes = make([dynamic]Route(S))
	index_init(&self.index)
}

deinit :: proc(self: ^Router($S)) {
	index_deinit(&self.index)
	builtin.delete(self.handlers)
	for route in self.routes {
		builtin.delete(route.frags)
		builtin.delete(route.handlers)
	}
	builtin.delete(self.routes)
}

use :: proc(router: ^Router($S), handler: proc(ctx: ^Context(S)) -> bool) {
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
	handler: proc(ctx: ^Context(S)) -> bool,
	mws: ..proc(ctx: ^Context(S)) -> bool,
) {
	Empty :: struct {}
	frags, err := parse_pattern(pattern, Empty)
	if err != .None do fmt.panicf("failed to parse static route '%s': %v", pattern, err)

	handlers := make([]proc(ctx: ^Context(S)) -> bool, len(mws))
	copy_slice(handlers, mws)

	r := Route(S) {
		method = method,
		frags = frags,
		handler = auto_cast handler,
		handlers = handlers,
		trampoline = proc(
			handler: rawptr,
			ctx: ^Context(S),
			_: string,
			_: []Fragment,
			_: []Param_Range,
		) -> bool {
			typed_handler := (proc(ctx: ^Context(S)) -> bool)(handler)
			return typed_handler(ctx)
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
	handler: proc(ctx: ^Context(S), params: ^T) -> bool,
	mws: ..proc(ctx: ^Context(S)) -> bool,
) {
	frags, err := parse_pattern(pattern, T)
	if err != .None do fmt.panicf("failed to parse route '%s': %v", pattern, err)

	handlers := make([]proc(ctx: ^Context(S)) -> bool, len(mws))
	copy_slice(handlers, mws)

	r := Route(S) {
		method = method,
		frags = frags,
		handler = auto_cast handler,
		handlers = handlers,
		trampoline = proc(
			handler: rawptr,
			ctx: ^Context(S),
			path: string,
			frags: []Fragment,
			ranges: []Param_Range,
		) -> bool {
			params: T
			if !deserialize_params(path, frags, ranges, &params) do return invalid_params(S)(ctx)

			typed_handler := (proc(ctx: ^Context(S), params: ^T) -> bool)(handler)
			return typed_handler(ctx, &params)
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
) -> ^Route(S) {
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

	handlers := make([dynamic]proc(ctx: ^Context(S)) -> bool, 0, len(self.handlers))
	defer builtin.delete(handlers)
	append_elems(&handlers, ..self.handlers[:])

	ds := Dispatch_State(S) {
		router   = self,
		handlers = handlers,
		idx      = 0,
		ranges   = ranges,
	}

	ds.target_route = find_route(self, req, &ranges)
	if ds.target_route != nil {
		append_elems(&handlers, ..ds.target_route.handlers)
		append(&handlers, endpoint_handler(S))
	} else do append(&handlers, not_found(S))

	ctx := Context(S) {
		state     = self.state,
		req       = req,
		res       = res,
		_internal = &ds,
	}

	return next(&ctx)
}

next :: proc(ctx: ^Context($S)) -> bool {
	ds := (^Dispatch_State(S))(ctx._internal)
	if ds.idx < len(ds.handlers) {
		handler := ds.handlers[ds.idx]
		ds.idx += 1
		return handler(ctx)
	}
	return true
}

@(private = "file")
endpoint_handler :: proc($S: typeid) -> proc(ctx: ^Context(S)) -> bool {
	return proc(ctx: ^Context(S)) -> bool {
			ds := (^Dispatch_State(S))(ctx._internal)
			if ds.target_route == nil do return not_found(S)(ctx)

			r := ds.target_route
			return r.trampoline(r.handler, ctx, ctx.req.uri, r.frags, ds.ranges[:])
		}
}

@(private = "file")
not_found :: proc($S: typeid) -> proc(ctx: ^Context(S)) -> bool {
	return proc(ctx: ^Context(S)) -> bool {
			return server.send_text(ctx.res, .Not_Found, "404 Not Found")
		}
}

@(private = "file")
invalid_params :: proc($S: typeid) -> proc(ctx: ^Context(S)) -> bool {
	return proc(ctx: ^Context(S)) -> bool {
			return server.send_text(ctx.res, .Internal_Server_Error, "Invalid Params")
		}
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

	HELLO :: proc(ctx: ^Context(State), params: ^Hello_Params) -> bool {
		return false
	}
	route_dyn(&r, .Get, "/hello/{name}", Hello_Params, HELLO)

	HOME :: proc(ctx: ^Context(State)) -> bool {
		return false
	}
	route_static(&r, .Get, "/", HOME)
}

