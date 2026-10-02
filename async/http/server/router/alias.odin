package async_http_server_router_v2

get :: proc {
	get_static,
	get_dyn,
}

get_static :: proc(
	self: ^Router($S),
	pattern: string,
	handler: proc(state: ^S, ctx: ^Context) -> bool,
	mws: ..Handler,
) {
	route_static(self, .Get, pattern, handler, ..mws)
}

get_dyn :: proc(
	self: ^Router($S),
	pattern: string,
	$T: typeid,
	handler: proc(state: ^S, ctx: ^Context, params: ^T) -> bool,
	mws: ..Handler,
) {
	route_dyn(self, .Get, pattern, T, handler, ..mws)
}

put :: proc {
	put_static,
	put_dyn,
}

put_static :: proc(
	self: ^Router($S),
	pattern: string,
	handler: proc(state: ^S, ctx: ^Context) -> bool,
	mws: ..Handler,
) {
	route_static(self, .Put, pattern, handler, ..mws)
}

put_dyn :: proc(
	self: ^Router($S),
	pattern: string,
	$T: typeid,
	handler: proc(state: ^S, ctx: ^Context, params: ^T) -> bool,
	mws: ..Handler,
) {
	route_dyn(self, .Put, pattern, T, handler, ..mws)
}

post :: proc {
	post_static,
	post_dyn,
}

post_static :: proc(
	self: ^Router($S),
	pattern: string,
	handler: proc(state: ^S, ctx: ^Context) -> bool,
	mws: ..Handler,
) {
	route_static(self, .Post, pattern, handler, ..mws)
}

post_dyn :: proc(
	self: ^Router($S),
	pattern: string,
	$T: typeid,
	handler: proc(state: ^S, ctx: ^Context, params: ^T) -> bool,
	mws: ..Handler,
) {
	route_dyn(self, .Post, pattern, T, handler, ..mws)
}

delete :: proc {
	delete_static,
	delete_dyn,
}

delete_static :: proc(
	self: ^Router($S),
	pattern: string,
	handler: proc(state: ^S, ctx: ^Context) -> bool,
	mws: ..Handler,
) {
	route_static(self, .Delete, pattern, handler, ..mws)
}

delete_dyn :: proc(
	self: ^Router($S),
	pattern: string,
	$T: typeid,
	handler: proc(state: ^S, ctx: ^Context, params: ^T) -> bool,
	mws: ..Handler,
) {
	route_dyn(self, .Delete, pattern, T, handler, ..mws)
}

