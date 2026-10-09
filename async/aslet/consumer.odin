package async_aslet

import "base:runtime"
import "core:strings"
import "core:sync/chan"
import "core:time"

import ".."
import "hl"

NO_TIMEOUT :: -1

@(private)
Pair :: struct($A: typeid, $B: typeid) {
	a: A,
	b: B,
}

Param :: hl.Param

Value :: hl.Value

Result :: hl.Result

Open_Flag :: hl.Open_Flag

Transaction_Mode :: hl.Transaction_Mode

Consumer :: struct {
	id:     u64,
	in_ch:  chan.Chan(^Operation),
	out_ch: chan.Chan(^Operation),
}

destroy :: proc(self: ^Consumer) {
	drain(self)

	ref := async.as_ref(self.id, Consumer)
	async.try_remove_ref(ref)

	chan.close(self.out_ch)
	chan.destroy(self.out_ch)

	free(self, allocator = runtime.default_allocator())
}

open :: proc(
	self: ^Consumer,
	path: string,
	open_flag: Open_Flag = .Create | .Read_Write | .No_Mutex,
) -> (
	conn: Conn,
	ok: bool,
) {
	cb :: proc(op: ^Operation) {
		os := transmute(async.One_Shot(Pair(rawptr, bool)))(op.ud[0])
		async.send(os, Pair(rawptr, bool){op.open.conn, op.open.ok})
	}

	os := async.create_one_shot(Pair(rawptr, bool))
	path := strings.clone(path)
	defer if !ok do delete(path)

	op := create_operation()
	op.out_ch = self.out_ch
	op.ud[0] = transmute(rawptr)(os)
	op.cb = cb
	op.type = .Open
	op.open = Open {
		path      = path,
		open_flag = open_flag,
	}
	chan.send(self.in_ch, op) or_return

	res := async.recv(os)
	if res.b do return Conn{self, res.a, path, open_flag}, true
	else do return {}, false
}

poll :: proc(self: ^Consumer, timeout: time.Duration = NO_TIMEOUT) {
	start := time.now()
	for {
		op := chan.try_recv(self.out_ch) or_break
		op.cb(op)
		if time.since(start) >= timeout do break
	}
}

drain :: proc(self: ^Consumer) {
	for {
		op := chan.try_recv(self.out_ch) or_break
		op.cb(op)
	}
}

@(private)
create_operation :: proc() -> ^Operation {
	return new(Operation, allocator = runtime.default_allocator())
}
