package async_http_server

import "base:runtime"
import "core:mem"
import "core:nbio"
import "core:net"
import "core:sync/chan"
import "core:sys/info"
import "core:thread"

import "../.."
import "../../io"

REQUEST_BUFFER_SIZE :: #config(HTTP_SERVER_REQUEST_SIZE, 4 * mem.Kilobyte)
RESPONSE_BUFFER_SIZE :: #config(HTTP_SERVER_RESPONSE_SIZE, 4 * mem.Kilobyte)
LINE_BUFFER_SIZE :: #config(HTTP_SERVER_LINE_SIZE, 4 * mem.Kilobyte)
TEMP_BUFFER_SIZE :: #config(HTTP_SERVER_TEMP_SIZE, 4 * mem.Kilobyte)
CONN_STACK_SIZE :: #config(HTTP_SERVER_STACK_SIZE, 4 * mem.Kilobyte)

Client :: struct {
	sock: nbio.TCP_Socket,
}

Request_Handler :: proc(state: rawptr, req: ^Request, res: ^Response) -> bool

Server :: struct {
	state:           rawptr,
	sock:            net.TCP_Socket,
	msgs:            chan.Chan(Receive_State),
	request_handler: Request_Handler,
	mime_types:      map[string]string,
	open:            bool,
	workers:         []^thread.Thread,
	cancel:          async.Cancel_Token,
	should_close:    bool,
}

init :: proc(
	self: ^Server,
	port: int,
	state: rawptr,
	request_handler: Request_Handler,
) -> (
	err: net.Network_Error,
) {
	endpoint := net.Endpoint{net.IP4_Any, port}
	self.sock = io.listen_tcp(endpoint) or_return
	self.state = state
	self.open = true
	self.request_handler = request_handler
	self.cancel = async.create_cancel_token()

	self.mime_types = make(map[string]string)
	init_mime_types(&self.mime_types)

	self.msgs, _ = chan.create_buffered(
		chan.Chan(Receive_State),
		1024,
		runtime.default_allocator(),
	)

	core_count, _, ok := info.cpu_core_count()
	self.workers = make([]^thread.Thread, ok ? core_count : 1)
	for i in 0 ..< len(self.workers) {
		self.workers[i] = thread.create_and_start_with_poly_data3(
			i,
			self.msgs,
			&self.should_close,
			worker,
		)
	}

	async.spawn(self, begin_accept, 64)
	return nil
}

close :: proc(self: ^Server) {
	async.trigger(self.cancel)
	self.should_close = true
	chan.close(&self.msgs)
}

deinit :: proc(self: ^Server) {
	for i in 0 ..< len(self.workers) do thread.destroy(self.workers[i])
	delete(self.workers)

	net.close(self.sock)
	deinit_mime_types(&self.mime_types)
}

@(private = "file")
begin_accept :: proc(self: ^Server) {
	for {
		sock, endpoint, err := io.accept(self.sock, cancel = self.cancel)
		if err != nil do break

		msg := Receive_State {
			sock            = sock,
			mime_types      = self.mime_types,
			shared_state    = self.state,
			request_handler = self.request_handler,
		}

		if !chan.send(self.msgs, msg) {
			io.close(self.sock)
			break
		}
	}
}

