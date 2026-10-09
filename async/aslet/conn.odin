package async_aslet

import ".."
import "hl"

Conn :: struct {
	aslet:     ^Consumer,
	conn:      rawptr,
	path:      string,
	open_flag: Open_Flag,
}

batch_insert :: proc(self: ^Conn, sql: string, params: [][]Param) -> Result {
	cb :: proc(rc: Result, ud: rawptr) {
		os := transmute(async.One_Shot(Result))(ud)
		async.send(os, rc)
	}

	os := async.create_one_shot(Result)
	ud := transmute(rawptr)(os)

	task := Task(Batch_Insert_Data, Batch_Insert_Callback) {
		out_ch = self.aslet.out_ch,
		ud = ud,
		cb = cb,
		data = Batch_Insert_Data{conn = self.conn, sql = sql, params = params},
	}

	ok := send(self.aslet, task)
	if !ok do return .Error

	return async.recv(os)

}

conn_exec :: proc(self: ^Conn, sql: string, params: []Param) -> Result {
	cb :: proc(rc: Result, ud: rawptr) {
		os := transmute(async.One_Shot(Result))(ud)
		async.send(os, rc)
	}

	os := async.create_one_shot(Result)
	ud := transmute(rawptr)(os)

	task := Task(Exec_Data, Exec_Callback) {
		out_ch = self.aslet.out_ch,
		ud = ud,
		cb = cb,
		data = Exec_Data{conn = self.conn, sql = sql, params = params},
	}

	ok := send(self.aslet, task)
	if !ok do return .Error

	return async.recv(os)

}

conn_fetch :: proc(
	self: ^Conn,
	sql: string,
	params: []Param = nil,
	out: ^[dynamic]$T,
	limit := 0,
) -> Result {
	cb :: proc(rc: Result, ud: rawptr) {
		os := transmute(async.One_Shot(Result))(ud)
		async.send(os, rc)
	}

	run :: proc(
		conn: ^hl.Conn,
		sql: string,
		params: []Param,
		out: rawptr,
		limit: int,
	) -> hl.Result {
		typed_out := (^[dynamic]T)(out)
		return hl.fetch(conn, sql, params, typed_out, limit)
	}

	os := async.create_one_shot(Result)
	ud := transmute(rawptr)(os)

	task := Task(Fetch_Data, Fetch_Callback) {
		out_ch = self.aslet.out_ch,
		ud = ud,
		cb = cb,
		data = Fetch_Data {
			conn = self.conn,
			sql = sql,
			params = params,
			out = out,
			limit = limit,
			run = run,
		},
	}

	ok := send(self.aslet, task)
	if !ok do return .Error

	return async.recv(os)
}

close :: proc(self: ^Conn) -> Result {
	cb :: proc(ud: rawptr) {
		handle := transmute(async.Handle)(ud)
		async.wake(handle)
	}

	delete(self.path)
	ud := transmute(rawptr)(async.get_handle())

	task := Task(Close_Data, Close_Callback) {
		out_ch = self.aslet.out_ch,
		ud = ud,
		cb = cb,
		data = Close_Data{conn = self.conn},
	}

	ok := send(self.aslet, task)
	if !ok do return .Error

	async.yield()
	return .Ok

}

Transaction :: struct {
	used: bool,
	conn: Conn,
}

transaction :: proc(
	self: ^Conn,
	mode: Transaction_Mode,
	ud: rawptr,
	cb: Transaction_Callback,
) -> (
	Transaction,
	bool,
) {
	cb :: proc(transaction: Transaction, ok: bool, ud: rawptr) {
		os := transmute(async.One_Shot(Pair(Transaction, bool)))(ud)
		async.send(os, Pair(Transaction, bool){transaction, ok})
	}

	os := async.create_one_shot(Pair(Transaction, bool))
	ud := transmute(rawptr)(os)

	task := Task(Transaction_Data, Transaction_Callback) {
		out_ch = self.aslet.out_ch,
		ud = ud,
		cb = cb,
		data = Transaction_Data{path = self.path, open_flag = self.open_flag, mode = mode},
	}

	ok := send(self.aslet, task)
	if !ok do return {}, false

	res := async.recv(os)
	return res.a, res.b

}

rollback :: proc(self: ^Transaction) -> (ok: bool) {
	if self.used do return false
	defer if ok do self.used = true

	cb :: proc(ok: bool, ud: rawptr) {
		os := transmute(async.One_Shot(bool))(ud)
		async.send(os, ok)
	}

	os := async.create_one_shot(bool)
	ud := transmute(rawptr)(os)

	task := Task(Rollback_Data, Rollback_Callback) {
		out_ch = self.conn.aslet.out_ch,
		ud = ud,
		cb = cb,
		data = Rollback_Data{conn = self.conn.conn},
	}

	send(self.conn.aslet, task) or_return
	return async.recv(os)
}

commit :: proc(self: ^Transaction) -> (ok: bool) {
	if self.used do return false
	defer if ok do self.used = true

	cb :: proc(ok: bool, ud: rawptr) {
		os := transmute(async.One_Shot(bool))(ud)
		async.send(os, ok)
	}

	os := async.create_one_shot(bool)
	ud := transmute(rawptr)(os)

	task := Task(Commit_Data, Commit_Callback) {
		out_ch = self.conn.aslet.out_ch,
		ud = ud,
		cb = cb,
		data = Commit_Data{conn = self.conn.conn},
	}

	send(self.conn.aslet, task) or_return
	return async.recv(os)
}

transaction_exec :: proc(self: ^Transaction, sql: string, params: []Param = nil) -> Result {
	if self.used do return .Abort
	return conn_exec(&self.conn, sql, params)
}

transaction_fetch :: proc(
	self: ^Transaction,
	sql: string,
	params: []Param,
	out: ^[dynamic]$T,
	limit := 0,
) -> Result {
	if self.used do return .Abort
	return conn_fetch(&self.conn, sql, params, out)
}

exec :: proc {
	conn_exec,
	transaction_exec,
}

fetch :: proc {
	conn_fetch,
	transaction_fetch,
}
