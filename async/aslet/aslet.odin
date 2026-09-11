package async_aslet

import "base:runtime"
import "core:strings"
import "core:sync/chan"
import "core:thread"
import "core:time"

import ".."
import "hl"

NO_TIMEOUT :: -1

@(private)
Pair :: struct($A: typeid, $B: typeid) {
	a: A,
	b: B,
}

Param :: hl.Param

Value :: hl.Value

Result :: hl.Result

Open_Flag :: hl.Open_Flag

Transaction_Mode :: hl.Transaction_Mode

Aslet :: struct {
	worker:          ^thread.Thread,
	input_sender:    chan.Chan(Request),
	output_receiver: chan.Chan(Response),
}

init :: proc(self: ^Aslet, max_tasks: int) -> (err: runtime.Allocator_Error) {
	input := chan.create_buffered(chan.Chan(Request), max_tasks, context.allocator) or_return
	defer if err != nil do chan.destroy(&input)

	output := chan.create_buffered(chan.Chan(Response), max_tasks, context.allocator) or_return
	defer if err != nil do chan.destroy(&output)

	self.input_sender = input
	self.output_receiver = output
	self.worker = thread.create_and_start_with_poly_data2(input, output, worker_run)

	return nil
}

deinit :: proc(self: ^Aslet) {
	chan.close(self.input_sender)
	thread.destroy(self.worker)

	drain(self)

	chan.close(self.output_receiver)
	chan.destroy(self.input_sender)
	chan.destroy(self.output_receiver)
}

open :: proc(
	self: ^Aslet,
	path: string,
	open_flag: Open_Flag = .Create | .Read_Write | .No_Mutex,
) -> (
	conn: Conn,
	ok: bool,
) {
	cb :: proc(conn: Conn, ok: bool, ud: rawptr) {
		handle := transmute(async.Handle)(ud)
		async.send(handle, Pair(Conn, bool){conn, ok})
	}

	path := strings.clone(path)
	ud := transmute(rawptr)(async.get_handle())

	ok = send(self, Open_Request{path = path, open_flag = open_flag, ud = ud, cb = cb})
	if !ok {
		delete(path)
		return {}, false
	}

	res := async.recv(Pair(Conn, bool))
	return res.a, res.b
}

poll :: proc(self: ^Aslet, timeout: time.Duration = NO_TIMEOUT) {
	start := time.now()
	for {
		msg := chan.try_recv(self.output_receiver) or_break
		dispatch(self, &msg)
		if time.since(start) >= timeout do break
	}
}

drain :: proc(self: ^Aslet) {
	for {
		resp := chan.try_recv(self.output_receiver) or_break
		dispatch(self, &resp)
	}
}

@(private)
dispatch :: proc(self: ^Aslet, resp: ^Response) {
	switch &m in resp {
	case Open_Response:
		if m.ok do m.cb(Conn{self, m.conn, m.path, m.open_flag}, m.ok, m.ud)
		else {
			delete(m.path)
			m.cb({}, m.ok, m.ud)
		}
	case Close_Response:
		m.cb(m.ud)
	case Exec_Response:
		m.cb(m.rc, m.ud)
	case Fetch_Response:
		m.cb(m.rc, m.ud)
	case Batch_Insert_Response:
		m.cb(m.rc, m.ud)
	case Transaction_Response:
		if m.ok {
			conn := Conn {
				aslet = self,
				conn  = m.conn,
			}
			m.cb(Transaction{false, conn}, true, m.ud)
		} else do m.cb({}, false, m.ud)
	case Rollback_Response:
		m.cb(m.ok, m.ud)
	case Commit_Response:
		m.cb(m.ok, m.ud)
	}
}

@(private)
send :: #force_inline proc(self: ^Aslet, req: Request) -> bool {
	return chan.send(self.input_sender, req)
}

