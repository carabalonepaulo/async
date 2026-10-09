package async_aslet

import ".."
import "hl"

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

prep_close :: proc(self: ^Consumer, conn: rawptr, cb: Callback) -> ^Operation {
	op := prep(self, .Close, cb)
	op.close = Close{conn}
	return op
}

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

prep_rollback :: proc(self: ^Consumer, conn: rawptr, cb: Callback) -> ^Operation {
	op := prep(self, .Rollback, cb)
	op.rollback = Rollback {
		conn = conn,
	}
	return op
}

prep_commit :: proc(self: ^Consumer, conn: rawptr, cb: Callback) -> ^Operation {
	op := prep(self, .Commit, cb)
	op.commit = Commit {
		conn = conn,
	}
	return op
}
