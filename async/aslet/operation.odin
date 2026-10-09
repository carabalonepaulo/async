package async_aslet

import "core:sync/chan"

import "hl"

MAX_USER_DATA :: 5

State :: enum {
	Pending,
	Canceled,
	Claimed,
}

Callback :: proc(op: ^Operation)

Operation :: struct {
	out_ch:          chan.Chan(^Operation),
	ud:              [MAX_USER_DATA]rawptr,
	cb:              Callback,
	state:           State,
	type:            Type,
	using specifics: Specifics,
	_link:           ^Operation,
}

Specifics :: struct #raw_union {
	open:         Open,
	close:        Close,
	batch_insert: Batch_Insert,
	exec:         Exec,
	fetch:        Fetch,
	transaction:  Transaction_OP,
	rollback:     Rollback,
	commit:       Commit,
}

Type :: enum {
	Open,
	Close,
	Batch_Insert,
	Exec,
	Fetch,
	Transaction,
	Rollback,
	Commit,
}

Open :: struct {
	// request
	consumer:  ^Consumer,
	path:      string,
	open_flag: Open_Flag,
	// response
	conn:      Conn,
	ok:        bool,
}

Close :: struct {
	conn: rawptr,
}

Batch_Insert :: struct {
	// request
	conn:   rawptr,
	sql:    string,
	params: [][]Param,
	// response
	rc:     Result,
}

Exec :: struct {
	// request
	conn:   rawptr,
	sql:    string,
	params: []Param,
	// response
	rc:     Result,
}

Fetch :: struct {
	// request
	conn:   rawptr,
	sql:    string,
	params: []Param,
	out:    rawptr,
	limit:  int,
	run:    proc(conn: ^hl.Conn, sql: string, params: []Param, out: rawptr, limit: int) -> Result,
	// response
	rc:     Result,
}

Transaction_OP :: struct {
	// request
	consumer:    ^Consumer,
	path:        string,
	open_flag:   Open_Flag,
	mode:        Transaction_Mode,
	// response
	transaction: Transaction,
	ok:          bool,
}

Rollback :: struct {
	// request
	conn: rawptr,
	// response
	ok:   bool,
}

Commit :: struct {
	// request
	conn: rawptr,
	// response
	ok:   bool,
}
