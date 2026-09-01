package aslet

import ".."
import "hl"

Conn :: struct {
	aslet:     ^Aslet,
	conn:      rawptr,
	path:      string,
	open_flag: Open_Flag,
}

batch_insert :: proc(self: ^Conn, sql: string, params: [][]Param) -> Result {
	cb :: proc(rc: Result, ud: rawptr) {
		handle := transmute(async.Handle)(ud)
		async.send(handle, rc)
	}

	ud := transmute(rawptr)(async.get_handle())
	ok := send(
		self.aslet,
		Batch_Insert_Request{conn = self.conn, sql = sql, params = params, ud = ud, cb = cb},
	)
	if !ok do return .Error

	return async.recv(Result)

}

conn_exec :: proc(self: ^Conn, sql: string, params: []Param) -> Result {
	cb :: proc(rc: Result, ud: rawptr) {
		handle := transmute(async.Handle)(ud)
		async.send(handle, rc)
	}

	ud := transmute(rawptr)(async.get_handle())
	ok := send(
		self.aslet,
		Exec_Request{conn = self.conn, sql = sql, params = params, ud = ud, cb = cb},
	)
	if !ok do return .Error

	return async.recv(Result)

}

conn_fetch :: proc(
	self: ^Conn,
	sql: string,
	params: []Param = nil,
	out: ^[dynamic]$T,
	limit := 0,
) -> Result {
	cb :: proc(rc: Result, ud: rawptr) {
		handle := transmute(async.Handle)(ud)
		async.send(handle, rc)
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

	ud := transmute(rawptr)(async.get_handle())
	ok := send(
		self.aslet,
		Fetch_Request {
			conn = self.conn,
			sql = sql,
			params = params,
			out = out,
			limit = limit,
			run = run,
			ud = ud,
			cb = cb,
		},
	)
	if !ok do return .Error

	return async.recv(Result)
}

close :: proc(self: ^Conn) -> Result {
	cb :: proc(ud: rawptr) {
		handle := transmute(async.Handle)(ud)
		async.wake(handle)
	}

	delete(self.path)
	ud := transmute(rawptr)(async.get_handle())
	ok := send(self.aslet, Close_Request{conn = self.conn, ud = ud, cb = cb})
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
		handle := transmute(async.Handle)(ud)
		async.send(handle, Pair(Transaction, bool){transaction, ok})
	}

	ud := transmute(rawptr)(async.get_handle())
	ok := send(
		self.aslet,
		Transaction_Request {
			path = self.path,
			open_flag = self.open_flag,
			mode = mode,
			ud = ud,
			cb = cb,
		},
	)
	if !ok do return {}, false

	res := async.recv(Pair(Transaction, bool))
	return res.a, res.b

}

rollback :: proc(self: ^Transaction) -> (ok: bool) {
	if self.used do return false
	defer if ok do self.used = true

	cb :: proc(ok: bool, ud: rawptr) {
		handle := transmute(async.Handle)(ud)
		async.send(handle, ok)
	}

	ud := transmute(rawptr)(async.get_handle())
	send(self.conn.aslet, Rollback_Request{conn = self.conn.conn, ud = ud, cb = cb}) or_return

	return async.recv(bool)
}

commit :: proc(self: ^Transaction) -> (ok: bool) {
	if self.used do return false
	defer if ok do self.used = true

	cb :: proc(ok: bool, ud: rawptr) {
		handle := transmute(async.Handle)(ud)
		async.send(handle, ok)
	}

	ud := transmute(rawptr)(async.get_handle())
	send(self.conn.aslet, Commit_Request{conn = self.conn.conn, ud = ud, cb = cb}) or_return

	return async.recv(bool)
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

