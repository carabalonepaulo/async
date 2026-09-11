package async_aslet

import "core:sync/chan"
import "hl"

@(private)
worker_run :: proc(input_ch: chan.Chan(Request), output_ch: chan.Chan(Response)) {
	for {
		msg := chan.recv(input_ch) or_break
		switch &m in msg {
		case Open_Request:
			on_open_request(&m, output_ch)
		case Close_Request:
			on_close_request(&m, output_ch)
		case Exec_Request:
			on_exec_request(&m, output_ch)
		case Fetch_Request:
			on_fetch_request(&m, output_ch)
		case Batch_Insert_Request:
			on_batch_insert_request(&m, output_ch)
		case Transaction_Request:
			on_transaction_request(&m, output_ch)
		case Rollback_Request:
			on_rollback_request(&m, output_ch)
		case Commit_Request:
			on_commit_request(&m, output_ch)
		}
	}
}

@(private = "file")
on_open_request :: proc(req: ^Open_Request, out_ch: chan.Chan(Response)) {
	conn := hl.open(req.path, req.open_flag)
	msg := Open_Response {
		path      = req.path,
		open_flag = req.open_flag,
		ud        = req.ud,
		cb        = req.cb,
	}

	if conn != nil {
		msg.ok = true
		msg.conn = conn
	}

	chan.send(out_ch, msg)
}

@(private = "file")
on_close_request :: proc(req: ^Close_Request, out_ch: chan.Chan(Response)) {
	hl.close((^hl.Conn)(req.conn))
	chan.send(out_ch, Close_Response{ud = req.ud, cb = req.cb})
}

@(private = "file")
on_exec_request :: proc(req: ^Exec_Request, out_ch: chan.Chan(Response)) {
	conn := (^hl.Conn)(req.conn)
	rc := hl.exec(conn, req.sql, req.params)
	chan.send(out_ch, Exec_Response{rc = rc, ud = req.ud, cb = req.cb})
}

@(private = "file")
on_fetch_request :: proc(req: ^Fetch_Request, out_ch: chan.Chan(Response)) {
	conn := (^hl.Conn)(req.conn)
	rc := req.run(conn, req.sql, req.params, req.out, req.limit)
	chan.send(out_ch, Fetch_Response{rc = rc, ud = req.ud, cb = req.cb})
}

@(private = "file")
on_batch_insert_request :: proc(req: ^Batch_Insert_Request, out_ch: chan.Chan(Response)) {
	conn := (^hl.Conn)(req.conn)
	rc := hl.batch_insert(conn, req.sql, req.params)
	chan.send(out_ch, Batch_Insert_Response{rc = rc, ud = req.ud, cb = req.cb})
}

@(private = "file")
on_transaction_request :: proc(req: ^Transaction_Request, out_ch: chan.Chan(Response)) {
	conn := hl.open(req.path, req.open_flag)
	resp := Transaction_Response {
		ok = false,
		ud = req.ud,
		cb = req.cb,
	}
	defer chan.send(out_ch, resp)
	if conn == nil do return

	if hl.begin(conn, req.mode) == .Ok {
		resp.conn = conn
		resp.ok = true
	} else do hl.close(conn)
}

@(private = "file")
on_rollback_request :: proc(req: ^Rollback_Request, out_ch: chan.Chan(Response)) {
	conn := (^hl.Conn)(req.conn)
	rc := hl.rollback(conn)
	hl.close(conn)
	chan.send(out_ch, Rollback_Response{ok = rc == .Ok, ud = req.ud, cb = req.cb})
}

@(private = "file")
on_commit_request :: proc(req: ^Commit_Request, out_ch: chan.Chan(Response)) {
	conn := (^hl.Conn)(req.conn)
	rc := hl.commit(conn)
	hl.close(conn)
	chan.send(out_ch, Commit_Response{ok = rc == .Ok, ud = req.ud, cb = req.cb})
}

