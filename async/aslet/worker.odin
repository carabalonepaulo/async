package async_aslet

import "base:intrinsics"
import "base:runtime"
import "core:container/pool"
import "core:mem"
import "core:sync/chan"
import "core:thread"

import ".."
import "hl"

DEFAULT_CAPACITY :: 1024

Worker :: struct {
	th:    ^thread.Thread,
	in_ch: chan.Chan(^Operation),
}

init :: proc(self: ^Worker, cap: int = DEFAULT_CAPACITY) -> (err: mem.Allocator_Error) {
	input := chan.create_buffered(chan.Chan(^Operation), cap, context.allocator) or_return
	defer if err != nil do chan.destroy(&input)

	self.in_ch = input
	self.th = thread.create_and_start_with_poly_data(input, worker_run)

	return .None
}

deinit :: proc(self: ^Worker) {
	chan.close(self.in_ch)
	thread.destroy(self.th)
	chan.destroy(self.in_ch)
}

create_consumer :: proc(
	self: ^Worker,
	cap: int = DEFAULT_CAPACITY,
) -> (
	consumer: ^Consumer,
	err: mem.Allocator_Error,
) {
	out_ch := chan.create_buffered(chan.Chan(^Operation), cap, context.allocator) or_return
	defer if err != nil do chan.destroy(&out_ch)

	op_pool: pool.Pool(Operation)
	pool.init(&op_pool, "_link") or_return
	defer if err != nil do pool.destroy(&op_pool)

	consumer = new(Consumer, runtime.default_allocator()) or_return
	ref := async.add_ref(consumer)
	consumer^ = Consumer{ref.id, op_pool, self.in_ch, out_ch}

	async.schedule(ref, proc(ref: async.Ref(Consumer)) -> bool {
		consumer := async.try_get_ref(ref) or_return
		poll(consumer)
		return true
	})

	return consumer, .None
}

@(private)
worker_run :: proc(input_ch: chan.Chan(^Operation)) {
	for {
		msg := chan.recv(input_ch) or_break

		_, ok := intrinsics.atomic_compare_exchange_strong(&msg.state, .Pending, .Done)
		if !ok {
			chan.send(msg.out_ch, msg)
			continue
		}

		switch msg.type {
		case .Open:
			on_open_request(&msg.open)
		case .Close:
			on_close_request(&msg.close)
		case .Batch_Insert:
			on_batch_insert_request(&msg.batch_insert)
		case .Exec:
			on_exec_request(&msg.exec)
		case .Fetch:
			on_fetch_request(&msg.fetch)
		case .Transaction:
			on_transaction_request(&msg.transaction)
		case .Rollback:
			on_rollback_request(&msg.rollback)
		case .Commit:
			on_commit_request(&msg.commit)
		}
		chan.send(msg.out_ch, msg)
	}
}

@(private = "file")
on_open_request :: proc(req: ^Open) {
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

@(private = "file")
on_close_request :: proc(req: ^Close) {
	hl.close((^hl.Conn)(req.conn))
}

@(private = "file")
on_exec_request :: proc(req: ^Exec) {
	conn := (^hl.Conn)(req.conn)
	req.rc = hl.exec(conn, req.sql, req.params)
}

@(private = "file")
on_fetch_request :: proc(req: ^Fetch) {
	conn := (^hl.Conn)(req.conn)
	req.rc = req.run(conn, req.sql, req.params, req.out, req.limit)
}

@(private = "file")
on_batch_insert_request :: proc(req: ^Batch_Insert) {
	conn := (^hl.Conn)(req.conn)
	req.rc = hl.batch_insert(conn, req.sql, req.params)
}

@(private = "file")
on_transaction_request :: proc(req: ^Transaction_OP) {
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

@(private = "file")
on_rollback_request :: proc(req: ^Rollback) {
	conn := (^hl.Conn)(req.conn)
	req.ok = hl.rollback(conn) == .Ok
	hl.close(conn)
}

@(private = "file")
on_commit_request :: proc(req: ^Commit) {
	conn := (^hl.Conn)(req.conn)
	req.ok = hl.commit(conn) == .Ok
	hl.close(conn)
}
