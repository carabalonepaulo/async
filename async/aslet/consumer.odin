package async_aslet

import "base:runtime"
import "core:strings"
import "core:sync/chan"
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

Consumer :: struct {
	id:     u64,
	in_ch:  chan.Chan(Request),
	out_ch: chan.Chan(Response),
}

destroy :: proc(self: ^Consumer) {
	drain(self)

	ref := async.as_ref(self.id, Consumer)
	async.try_remove_ref(ref)

	chan.close(self.out_ch)
	chan.destroy(self.out_ch)

	free(self, allocator = runtime.default_allocator())
}

open :: proc(
	self: ^Consumer,
	path: string,
	open_flag: Open_Flag = .Create | .Read_Write | .No_Mutex,
) -> (
	conn: Conn,
	ok: bool,
) {
	cb :: proc(conn: Conn, ok: bool, ud: rawptr) {
		os := transmute(async.One_Shot(Pair(Conn, bool)))(ud)
		async.send(os, Pair(Conn, bool){conn, ok})
	}

	path := strings.clone(path)
	os := async.create_one_shot(Pair(Conn, bool))
	ud := transmute(rawptr)(os)

	task := Task(Open_Data, Open_Callback) {
		out_ch = self.out_ch,
		ud = ud,
		cb = cb,
		data = Open_Data{path = path, open_flag = open_flag},
	}

	ok = send(self, task)
	if !ok {
		delete(path)
		return {}, false
	}

	res := async.recv(os)
	return res.a, res.b
}

poll :: proc(self: ^Consumer, timeout: time.Duration = NO_TIMEOUT) {
	start := time.now()
	for {
		msg := chan.try_recv(self.out_ch) or_break
		dispatch(self, &msg)
		if time.since(start) >= timeout do break
	}
}

drain :: proc(self: ^Consumer) {
	for {
		resp := chan.try_recv(self.out_ch) or_break
		dispatch(self, &resp)
	}
}

@(private)
dispatch :: proc(self: ^Consumer, resp: ^Response) {
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
send :: #force_inline proc(self: ^Consumer, req: Request) -> bool {
	return chan.send(self.in_ch, req)
}
