package async_aslet

import "base:intrinsics"
import "base:runtime"
import "core:container/pool"
import "core:mem"
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
prep :: proc(
	self: ^Consumer,
	type: Type,
	cb: Callback,
	run: proc(spec: ^Specifics),
) -> ^Operation {
	self.pending += 1
	op := pool.get(self.pool)
	op.run = run
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
	run :: proc(req: ^Open) {
		conn := hl.open(req.path, req.open_flag)
		if conn != nil {
			req.ok = true
			req.conn = Conn {
				aslet     = req.consumer,
				conn      = conn,
				path      = req.path,
				open_flag = req.open_flag,
			}
		}
	}

	op := prep(self, .Open, cb, auto_cast run)
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
	run :: proc(req: ^Batch_Insert) {
		conn := (^hl.Conn)(req.conn)
		req.rc = hl.batch_insert(conn, req.sql, req.params)
	}

	op := prep(self, .Batch_Insert, cb, auto_cast run)
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
	run :: proc(req: ^Exec) {
		conn := (^hl.Conn)(req.conn)
		req.rc = hl.exec(conn, req.sql, req.params)
	}

	op := prep(self, .Exec, cb, auto_cast run)
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
	run :: proc(req: ^Fetch) {
		req.rc = hl.fetch(
			(^hl.Conn)(req.conn),
			req.sql,
			req.params,
			(^[dynamic]T)(req.out),
			req.limit,
		)
	}

	op := prep(self, .Fetch, cb, auto_cast run)
	op.fetch = Fetch {
		conn   = conn,
		sql    = sql,
		params = params,
		out    = out,
		limit  = limit,
	}
	return op
}

@(private)
prep_fetch_single :: proc(
	self: ^Consumer,
	conn: rawptr,
	sql: string,
	params: []Param,
	out: ^$T,
	cb: Callback,
	allocator: mem.Allocator,
) -> ^Operation {
	run :: proc(req: ^Fetch) {
		req.rc = hl.fetch_single(
			(^hl.Conn)(req.conn),
			req.sql,
			req.params,
			(^T)(req.out),
			req.allocator,
		)
	}

	op := prep(self, .Fetch_Single, cb, auto_cast run)
	op.fetch_single = Fetch {
		conn      = conn,
		sql       = sql,
		params    = params,
		out       = out,
		allocator = allocator,
	}
	return op
}

@(private)
prep_fetch_scalar :: proc(
	self: ^Consumer,
	conn: rawptr,
	sql: string,
	params: []Param,
	out: ^$T,
	cb: Callback,
	allocator: mem.Allocator,
) -> ^Operation {
	run :: proc(req: ^Fetch) {
		req.rc = hl.fetch_scalar(
			(^hl.Conn)(req.conn),
			req.sql,
			req.params,
			(^T)(req.out),
			req.allocator,
		)
	}

	op := prep(self, .Fetch_Scalar, cb, auto_cast run)
	op.fetch_scalar = Fetch {
		conn      = conn,
		sql       = sql,
		params    = params,
		out       = out,
		allocator = allocator,
	}
	return op
}

@(private)
prep_close :: proc(self: ^Consumer, conn: rawptr, cb: Callback) -> ^Operation {
	run :: proc(req: ^Close) {
		hl.close((^hl.Conn)(req.conn))
	}

	op := prep(self, .Close, cb, auto_cast run)
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
	run :: proc(req: ^Transaction_OP) {
		conn := hl.open(req.path, req.open_flag)
		if conn == nil do return

		if hl.begin(conn, req.mode) == .Ok {
			req.transaction = Transaction {
				used = false,
				conn = Conn{aslet = req.consumer, conn = conn},
			}
			req.ok = true
		} else do hl.close(conn)
	}

	op := prep(self, .Transaction, cb, auto_cast run)
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
	run :: proc(req: ^Rollback) {
		conn := (^hl.Conn)(req.conn)
		req.ok = hl.rollback(conn) == .Ok
		hl.close(conn)
	}

	op := prep(self, .Rollback, cb, auto_cast run)
	op.rollback = Rollback {
		conn = conn,
	}
	return op
}

@(private)
prep_commit :: proc(self: ^Consumer, conn: rawptr, cb: Callback) -> ^Operation {
	run :: proc(req: ^Commit) {
		conn := (^hl.Conn)(req.conn)
		req.ok = hl.commit(conn) == .Ok
		hl.close(conn)
	}

	op := prep(self, .Commit, cb, auto_cast run)
	op.commit = Commit {
		conn = conn,
	}
	return op
}
