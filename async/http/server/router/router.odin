package async_http_server_router_v2

import "base:builtin"
import "core:fmt"
import "core:testing"

import server ".."

Route :: struct($S: typeid, $L: typeid) {
	method:     server.Method,
	frags:      []Fragment,
	handler:    rawptr,
	handlers:   []proc(ctx: ^Context(S, L)) -> bool,
	trampoline: proc(
		handler_ptr: rawptr,
		ctx: ^Context(S, L),
		path: string,
		frags: []Fragment,
		match_ctx: []Param_Range,
	) -> bool,
}

Dispatch_State :: struct($S: typeid, $L: typeid) {
	router:       ^Router(S, L),
	handlers:     [dynamic]proc(ctx: ^Context(S, L)) -> bool,
	idx:          int,
	target_route: ^Route(S, L),
	ranges:       [dynamic]Param_Range,
}

Context :: struct($S: typeid, $L: typeid) {
	state:     ^S,
	local:     ^L,
	req:       ^server.Request,
	res:       ^server.Response,
	_internal: rawptr,
}

Router :: struct($S: typeid, $L: typeid) {
	state:    ^S,
	handlers: [dynamic]proc(ctx: ^Context(S, L)) -> bool,
	routes:   [dynamic]Route(S, L),
	index:    Index,
}

init :: proc(self: ^Router($S, $L), state: ^S) {
	self.state = state
	self.handlers = make([dynamic]proc(ctx: ^Context(S, L)) -> bool)
	self.routes = make([dynamic]Route(S, L))
	index_init(&self.index)
}

deinit :: proc(self: ^Router($S, $L)) {
	index_deinit(&self.index)
	builtin.delete(self.handlers)
	for route in self.routes {
		builtin.delete(route.frags)
		builtin.delete(route.handlers)
	}
	builtin.delete(self.routes)
}

use :: proc(router: ^Router($S, $L), handler: proc(ctx: ^Context(S, L)) -> bool) {
	append(&router.handlers, handler)
}

route :: proc {
	route_static,
	route_dyn,
}

route_static :: proc(
	self: ^Router($S, $L),
	method: server.Method,
	pattern: string,
	handler: proc(ctx: ^Context(S, L)) -> bool,
	mws: ..proc(ctx: ^Context(S, L)) -> bool,
) {
	Empty :: struct {}
	frags, err := parse_pattern(pattern, Empty)
	if err != .None do fmt.panicf("failed to parse static route '%s': %v", pattern, err)

	handlers := make([]proc(ctx: ^Context(S, L)) -> bool, len(mws))
	copy_slice(handlers, mws)

	r := Route(S, L) {
		method = method,
		frags = frags,
		handler = auto_cast handler,
		handlers = handlers,
		trampoline = proc(
			handler: rawptr,
			ctx: ^Context(S, L),
			_: string,
			_: []Fragment,
			_: []Param_Range,
		) -> bool {
			typed_handler := (proc(ctx: ^Context(S, L)) -> bool)(handler)
			return typed_handler(ctx)
		},
	}

	index_add(&self.index, len(self.routes), pattern)
	append(&self.routes, r)
}

route_dyn :: proc(
	self: ^Router($S, $L),
	method: server.Method,
	pattern: string,
	$T: typeid,
	handler: proc(ctx: ^Context(S, L), params: ^T) -> bool,
	mws: ..proc(ctx: ^Context(S, L)) -> bool,
) {
	frags, err := parse_pattern(pattern, T)
	if err != .None do fmt.panicf("failed to parse route '%s': %v", pattern, err)

	handlers := make([]proc(ctx: ^Context(S, L)) -> bool, len(mws))
	copy_slice(handlers, mws)

	r := Route(S, L) {
		method = method,
		frags = frags,
		handler = auto_cast handler,
		handlers = handlers,
		trampoline = proc(
			handler: rawptr,
			ctx: ^Context(S, L),
			path: string,
			frags: []Fragment,
			ranges: []Param_Range,
		) -> bool {
			params: T
			if !deserialize_params(path, frags, ranges, &params) do return invalid_params(S, L)(ctx)

			typed_handler := (proc(ctx: ^Context(S, L), params: ^T) -> bool)(handler)
			return typed_handler(ctx, &params)
		},
	}

	index_add(&self.index, len(self.routes), pattern)
	append(&self.routes, r)
}

dispatch :: proc(self: ^Router($S, $L)) -> server.Request_Handler {
	return proc(ud: rawptr, req: ^server.Request, res: ^server.Response) -> bool {
			self := (^Router(S, L))(ud)
			return _dispatch(self, req, res)
		}
}

@(private = "file")
find_route :: proc(
	self: ^Router($S, $L),
	req: ^server.Request,
	ranges: ^[dynamic]Param_Range,
) -> ^Route(S, L) {
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
_dispatch :: proc(self: ^Router($S, $L), req: ^server.Request, res: ^server.Response) -> bool {
	ranges := make([dynamic]Param_Range, 0, 4)
	defer builtin.delete(ranges)

	handlers := make([dynamic]proc(ctx: ^Context(S, L)) -> bool, 0, len(self.handlers))
	defer builtin.delete(handlers)
	append_elems(&handlers, ..self.handlers[:])

	target_route := find_route(self, req, &ranges)
	if target_route != nil {
		append_elems(&handlers, ..target_route.handlers)
		append(&handlers, endpoint_handler(S, L))
	} else do append(&handlers, not_found(S, L))

	ds := Dispatch_State(S, L) {
		router       = self,
		target_route = target_route,
		handlers     = handlers,
		idx          = 0,
		ranges       = ranges,
	}

	local: L = {}

	ctx := Context(S, L) {
		state     = self.state,
		local     = &local,
		req       = req,
		res       = res,
		_internal = &ds,
	}

	return next(&ctx)
}

next :: proc(ctx: ^Context($S, $L)) -> bool {
	ds := (^Dispatch_State(S, L))(ctx._internal)
	if ds.idx < len(ds.handlers) {
		handler := ds.handlers[ds.idx]
		ds.idx += 1
		return handler(ctx)
	}
	return true
}

@(private = "file")
endpoint_handler :: proc($S: typeid, $L: typeid) -> proc(ctx: ^Context(S, L)) -> bool {
	return proc(ctx: ^Context(S, L)) -> bool {
			ds := (^Dispatch_State(S, L))(ctx._internal)
			if ds.target_route == nil do return not_found(S, L)(ctx)

			r := ds.target_route
			return r.trampoline(r.handler, ctx, ctx.req.uri, r.frags, ds.ranges[:])
		}
}

@(private = "file")
not_found :: proc($S: typeid, $L: typeid) -> proc(ctx: ^Context(S, L)) -> bool {
	return proc(ctx: ^Context(S, L)) -> bool {
			return server.send_text(ctx.res, .Not_Found, "404 Not Found")
		}
}

@(private = "file")
invalid_params :: proc($S: typeid, $L: typeid) -> proc(ctx: ^Context(S, L)) -> bool {
	return proc(ctx: ^Context(S, L)) -> bool {
			return server.send_text(ctx.res, .Internal_Server_Error, "Invalid Params")
		}
}

@(test)
test_defining_route :: proc(t: ^testing.T) {
	State :: struct {
		t: ^testing.T,
	}

	Local :: struct {
		name: string,
	}

	state := State{t}

	r: Router(State, Local)
	init(&r, &state)
	defer deinit(&r)

	Hello_Params :: struct {
		name: string,
	}

	mw :: proc(ctx: ^Context(State, Local)) -> bool {
		ctx.local.name = "soreto"
		return next(ctx)
	}

	HELLO :: proc(ctx: ^Context(State, Local), params: ^Hello_Params) -> bool {
		return false
	}
	route_dyn(&r, .Get, "/hello/{name}", Hello_Params, HELLO)

	HOME :: proc(ctx: ^Context(State, Local)) -> bool {
		return false
	}
	route_static(&r, .Get, "/", HOME)
}
