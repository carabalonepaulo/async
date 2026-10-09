package async_aslet

import "base:intrinsics"
import "base:runtime"
import "core:container/pool"
import "core:sync/chan"

import ".."
import "hl"

@(private)
send :: proc(self: ^Consumer, op: ^Operation) -> (ok: bool) {
	defer if !ok do release_operation(self, op)
	return chan.send(self.in_ch, op)
}

@(private)
release_operation :: proc(self: ^Consumer, op: ^Operation) {
	self.pending -= 1
	pool.put(self.pool, op)
}

@(private)
try :: proc(
	op: ^Operation,
	cancel: Maybe(async.Cancel_Token),
	$T: typeid,
	ok: $E,
	err: E,
	loc := #caller_location,
) -> (
	T,
	E,
) {
	os := async.create_one_shot(T)
	op.ud[0] = transmute(rawptr)(os)

	if cancel, cancel_ok := cancel.(async.Cancel_Token); cancel_ok {
		res: T
		idx := async.select({async.branch(cancel), async.branch(os, &res)})

		if idx == 1 do return res, ok

		_, ok := intrinsics.atomic_compare_exchange_strong(&op.state, .Pending, .Canceled)
		if ok {
			async.destroy(os)
			return {}, err
		}
	}

	res := async.recv(os)
	return res, ok
}

@(private)
prep :: proc(self: ^Consumer, type: Type, cb: Callback) -> ^Operation {
	self.pending += 1
	op := pool.get(self.pool)
	op.out_ch = self.out_ch
	op.type = type
	op.cb = cb
	op.state = .Pending
	return op
}

@(private)
prep_open :: proc(
	self: ^Consumer,
	path: string,
	open_flag: Open_Flag,
	cb: Callback,
) -> ^Operation {
	op := prep(self, .Open, cb)
	op.open = Open {
		consumer  = self,
		path      = path,
		open_flag = open_flag,
	}
	return op
}

@(private)
prep_batch_insert :: proc(
	self: ^Consumer,
	conn: rawptr,
	sql: string,
	params: [][]Param,
	cb: Callback,
) -> ^Operation {
	op := prep(self, .Batch_Insert, cb)
	op.batch_insert = Batch_Insert {
		conn   = conn,
		sql    = sql,
		params = params,
	}
	return op
}

@(private)
prep_exec :: proc(
	self: ^Consumer,
	conn: rawptr,
	sql: string,
	params: []Param,
	cb: Callback,
) -> ^Operation {
	op := prep(self, .Exec, cb)
	op.exec = Exec {
		conn   = conn,
		sql    = sql,
		params = params,
	}
	return op
}

@(private)
prep_fetch :: proc(
	self: ^Consumer,
	conn: rawptr,
	sql: string,
	params: []Param,
	out: ^[dynamic]$T,
	limit: int,
	cb: Callback,
) -> ^Operation {
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

	op := prep(self, .Fetch, cb)
	op.fetch = Fetch {
		conn   = conn,
		sql    = sql,
		params = params,
		out    = out,
		limit  = limit,
		run    = run,
	}
	return op
}

@(private)
prep_close :: proc(self: ^Consumer, conn: rawptr, cb: Callback) -> ^Operation {
	op := prep(self, .Close, cb)
	op.close = Close{conn}
	return op
}

@(private)
prep_transaction :: proc(
	self: ^Consumer,
	path: string,
	open_flag: Open_Flag,
	mode: Transaction_Mode,
	cb: Callback,
) -> ^Operation {
	op := prep(self, .Transaction, cb)
	op.transaction = Transaction_OP {
		consumer  = self,
		path      = path,
		open_flag = open_flag,
		mode      = mode,
	}
	return op
}

@(private)
prep_rollback :: proc(self: ^Consumer, conn: rawptr, cb: Callback) -> ^Operation {
	op := prep(self, .Rollback, cb)
	op.rollback = Rollback {
		conn = conn,
	}
	return op
}

@(private)
prep_commit :: proc(self: ^Consumer, conn: rawptr, cb: Callback) -> ^Operation {
	op := prep(self, .Commit, cb)
	op.commit = Commit {
		conn = conn,
	}
	return op
}
