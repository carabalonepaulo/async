package async_aslet

import "core:sync/chan"
import "hl"

Task_State :: enum {
	Pending,
	Canceled,
	Done,
}

Task :: struct($T: typeid, $C: typeid) {
	out_ch: chan.Chan(Response),
	state:  ^Task_State,
	data:   T,
	ud:     rawptr,
	cb:     C,
}

Open_Callback :: proc(conn: Conn, ok: bool, ud: rawptr)

Open_Data :: struct {
	path:      string,
	open_flag: Open_Flag,
}

Open_Request :: Task(Open_Data, Open_Callback)

Close_Callback :: proc(ud: rawptr)

Close_Data :: struct {
	conn: rawptr,
}

Close_Request :: Task(Close_Data, Close_Callback)

Batch_Insert_Callback :: proc(rc: Result, ud: rawptr)

Batch_Insert_Data :: struct {
	conn:   rawptr,
	sql:    string,
	params: [][]Param,
}

Batch_Insert_Request :: Task(Batch_Insert_Data, Batch_Insert_Callback)

Exec_Callback :: proc(rc: Result, ud: rawptr)

Exec_Data :: struct {
	conn:   rawptr,
	sql:    string,
	params: []Param,
}

Exec_Request :: Task(Exec_Data, Exec_Callback)

Fetch_Callback :: proc(rc: Result, ud: rawptr)

Fetch_Data :: struct {
	conn:   rawptr,
	sql:    string,
	params: []Param,
	out:    rawptr,
	limit:  int,
	run:    proc(conn: ^hl.Conn, sql: string, params: []Param, out: rawptr, limit: int) -> Result,
}

Fetch_Request :: Task(Fetch_Data, Fetch_Callback)

Transaction_Callback :: proc(transaction: Transaction, ok: bool, ud: rawptr)

Transaction_Data :: struct {
	path:      string,
	open_flag: Open_Flag,
	mode:      Transaction_Mode,
}

Transaction_Request :: Task(Transaction_Data, Transaction_Callback)

Rollback_Callback :: proc(ok: bool, ud: rawptr)

Rollback_Data :: struct {
	conn: rawptr,
}

Rollback_Request :: Task(Rollback_Data, Rollback_Callback)

Commit_Callback :: proc(ok: bool, ud: rawptr)

Commit_Data :: struct {
	out_ch: chan.Chan(Response),
	conn:   rawptr,
	ud:     rawptr,
	cb:     Commit_Callback,
}

Commit_Request :: Task(Commit_Data, Commit_Callback)

Request :: union {
	Open_Request,
	Close_Request,
	Batch_Insert_Request,
	Exec_Request,
	Fetch_Request,
	Transaction_Request,
	Rollback_Request,
	Commit_Request,
}

Open_Response :: struct {
	path:      string,
	open_flag: Open_Flag,
	conn:      rawptr,
	ok:        bool,
	ud:        rawptr,
	cb:        Open_Callback,
}

Close_Response :: struct {
	ud: rawptr,
	cb: Close_Callback,
}

Batch_Insert_Response :: struct {
	rc: Result,
	ud: rawptr,
	cb: Batch_Insert_Callback,
}

Exec_Response :: struct {
	rc: Result,
	ud: rawptr,
	cb: Exec_Callback,
}

Fetch_Response :: struct {
	rc: Result,
	ud: rawptr,
	cb: Fetch_Callback,
}

Transaction_Response :: struct {
	conn: rawptr,
	ok:   bool,
	ud:   rawptr,
	cb:   Transaction_Callback,
}

Rollback_Response :: struct {
	ok: bool,
	ud: rawptr,
	cb: Rollback_Callback,
}

Commit_Response :: struct {
	ok: bool,
	ud: rawptr,
	cb: Commit_Callback,
}

Response :: union {
	Open_Response,
	Close_Response,
	Batch_Insert_Response,
	Exec_Response,
	Fetch_Response,
	Transaction_Response,
	Rollback_Response,
	Commit_Response,
}
