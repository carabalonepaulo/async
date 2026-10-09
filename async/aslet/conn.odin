package async_aslet

import ".."

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
	op := prep_batch_insert(self.aslet, self.conn, sql, params, cb)
	op.ud[0] = transmute(rawptr)(os)

	if !send(self.aslet, op) do return .Error
	return async.recv(os)
}

conn_exec :: proc(self: ^Conn, sql: string, params: []Param) -> Result {
	cb :: proc(op: ^Operation) {
		os := transmute(async.One_Shot(Result))(op.ud[0])
		async.send(os, op.exec.rc)
	}

	os := async.create_one_shot(Result)
	op := prep_exec(self.aslet, self.conn, sql, params, cb)
	op.ud[0] = transmute(rawptr)(os)

	if !send(self.aslet, op) do return .Error
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

	os := async.create_one_shot(Result)
	op := prep_fetch(self.aslet, self.conn, sql, params, out, limit, cb)
	op.ud[0] = transmute(rawptr)(os)

	if !send(self.aslet, op) do return .Error
	return async.recv(os)
}

close :: proc(self: ^Conn) -> Result {
	cb :: proc(op: ^Operation) {
		handle := transmute(async.Handle)(op.ud[0])
		async.wake(handle)
	}

	delete(self.path)

	op := prep_close(self.aslet, self.conn, cb)
	op.ud[0] = transmute(rawptr)(async.get_handle())

	if !send(self.aslet, op) do return .Error
	async.yield()
	return .Ok
}

Transaction :: struct {
	used: bool,
	conn: Conn,
}

transaction :: proc(self: ^Conn, mode: Transaction_Mode) -> (transaction: Transaction, ok: bool) {
	cb :: proc(op: ^Operation) {
		os := transmute(async.One_Shot(Pair(Transaction, bool)))(op.ud[0])
		async.send(os, Pair(Transaction, bool){op.transaction.transaction, op.transaction.ok})
	}

	os := async.create_one_shot(Pair(Transaction, bool))
	op := prep_transaction(self.aslet, self.path, self.open_flag, mode, cb)
	op.ud[0] = transmute(rawptr)(os)

	send(self.aslet, op) or_return
	res := async.recv(os)
	return res.a, res.b
}

rollback :: proc(self: ^Transaction) -> (ok: bool) {
	if self.used do return false
	defer if ok do self.used = true

	cb :: proc(op: ^Operation) {
		os := transmute(async.One_Shot(bool))(op.ud[0])
		async.send(os, op.rollback.ok)
	}

	os := async.create_one_shot(bool)
	op := prep_rollback(self.conn.aslet, self.conn.conn, cb)
	op.ud[0] = transmute(rawptr)(os)

	send(self.conn.aslet, op) or_return
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
	op := prep_commit(self.conn.aslet, self.conn.conn, cb)
	op.ud[0] = transmute(rawptr)(os)

	send(self.conn.aslet, op) or_return
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
