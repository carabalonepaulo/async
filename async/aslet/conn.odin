package async_aslet

import ".."
import "core:sync/chan"
import "hl"

Conn :: struct {
	aslet:     ^Consumer,
	conn:      rawptr,
	path:      string,
	open_flag: Open_Flag,
}

batch_insert :: proc(self: ^Conn, sql: string, params: [][]Param) -> Result {
	cb :: proc(op: ^Operation) {
		os := transmute(async.One_Shot(Result))(op.ud[0])
		async.send(os, op.batch_insert.rc)
	}

	os := async.create_one_shot(Result)
	op := prep(self.aslet, .Batch_Insert, cb)
	op.ud[0] = transmute(rawptr)(os)
	op.batch_insert = Batch_Insert {
		conn   = self.conn,
		sql    = sql,
		params = params,
	}

	if !chan.send(self.aslet.in_ch, op) do return .Error
	return async.recv(os)
}

conn_exec :: proc(self: ^Conn, sql: string, params: []Param) -> Result {
	cb :: proc(op: ^Operation) {
		os := transmute(async.One_Shot(Result))(op.ud[0])
		async.send(os, op.exec.rc)
	}

	os := async.create_one_shot(Result)
	op := prep(self.aslet, .Exec, cb)
	op.ud[0] = transmute(rawptr)(os)
	op.exec = Exec {
		conn   = self.conn,
		sql    = sql,
		params = params,
	}

	if !chan.send(self.aslet.in_ch, op) do return .Error
	return async.recv(os)
}

conn_fetch :: proc(
	self: ^Conn,
	sql: string,
	params: []Param = nil,
	out: ^[dynamic]$T,
	limit := 0,
) -> Result {
	cb :: proc(op: ^Operation) {
		os := transmute(async.One_Shot(Result))(op.ud[0])
		async.send(os, op.fetch.rc)
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
	op := prep(self.aslet, .Fetch, cb)
	op.ud[0] = transmute(rawptr)(os)
	op.fetch = Fetch {
		conn   = self.conn,
		sql    = sql,
		params = params,
		out    = out,
		limit  = limit,
		run    = run,
	}

	if !chan.send(self.aslet.in_ch, op) do return .Error
	return async.recv(os)
}

close :: proc(self: ^Conn) -> Result {
	cb :: proc(op: ^Operation) {
		handle := transmute(async.Handle)(op.ud[0])
		async.wake(handle)
	}

	delete(self.path)

	op := prep(self.aslet, .Close, cb)
	op.ud[0] = transmute(rawptr)(async.get_handle())
	op.close = Close{self.conn}
	if !chan.send(self.aslet.in_ch, op) do return .Error

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
) -> (
	transaction: Transaction,
	ok: bool,
) {
	cb :: proc(op: ^Operation) {
		os := transmute(async.One_Shot(Pair(rawptr, bool)))(op.ud[0])
		async.send(os, Pair(rawptr, bool){op.transaction.conn, op.transaction.ok})
	}

	os := async.create_one_shot(Pair(rawptr, bool))
	op := prep(self.aslet, .Transaction, cb)
	op.ud[0] = transmute(rawptr)(os)
	op.transaction = Transaction_OP {
		path      = self.path,
		open_flag = self.open_flag,
		mode      = mode,
	}

	chan.send(self.aslet.in_ch, op) or_return
	res := async.recv(os)
	if res.b {
		conn := Conn {
			aslet = self.aslet,
			conn  = res.a,
		}
		return Transaction{false, conn}, true
	} else do return {}, false
}

rollback :: proc(self: ^Transaction) -> (ok: bool) {
	if self.used do return false
	defer if ok do self.used = true

	cb :: proc(op: ^Operation) {
		os := transmute(async.One_Shot(bool))(op.ud[0])
		async.send(os, op.rollback.ok)
	}

	os := async.create_one_shot(bool)
	op := prep(self.conn.aslet, .Rollback, cb)
	op.ud[0] = transmute(rawptr)(os)
	op.rollback = Rollback {
		conn = self.conn.conn,
	}

	chan.send(self.conn.aslet.in_ch, op) or_return
	return async.recv(os)
}

commit :: proc(self: ^Transaction) -> (ok: bool) {
	if self.used do return false
	defer if ok do self.used = true

	cb :: proc(op: ^Operation) {
		os := transmute(async.One_Shot(bool))(op.ud[0])
		async.send(os, op.commit.ok)
	}

	os := async.create_one_shot(bool)
	op := prep(self.conn.aslet, .Commit, cb)
	op.ud[0] = transmute(rawptr)(os)
	op.commit = Commit {
		conn = self.conn.conn,
	}

	chan.send(self.conn.aslet.in_ch, op) or_return
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
