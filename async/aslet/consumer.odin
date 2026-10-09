package async_aslet

import "base:runtime"
import "core:container/pool"
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
	id:      u64,
	pool:    ^pool.Pool(Operation),
	in_ch:   chan.Chan(^Operation),
	out_ch:  chan.Chan(^Operation),
	pending: int,
}

destroy :: proc(self: ^Consumer) {
	drain(self)
	assert(self.pending == 0)

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
	cancel: Maybe(async.Cancel_Token) = nil,
) -> (
	conn: Conn,
	ok: bool,
) {
	cb :: proc(op: ^Operation) {
		os := transmute(async.One_Shot(Pair(Conn, bool)))(op.ud[0])
		async.send(os, Pair(Conn, bool){op.open.conn, op.open.ok})
	}

	path := strings.clone(path)
	defer if !ok do delete(path)

	op := prep_open(self, path, open_flag, cb)
	send(self, op) or_return

	res := try(op, cancel, Pair(Conn, bool), true, false) or_return
	return res.a, res.b
}

poll :: proc(self: ^Consumer, timeout: time.Duration = NO_TIMEOUT) {
	start := time.now()
	for {
		op := chan.try_recv(self.out_ch) or_break
		dispatch(self, op)
		if time.since(start) >= timeout do break
	}
}

drain :: proc(self: ^Consumer) {
	for {
		op := chan.try_recv(self.out_ch) or_break
		dispatch(self, op)
	}
}

@(private)
dispatch :: proc(self: ^Consumer, op: ^Operation) {
	if op.state == .Claimed do op.cb(op)
	release_operation(self, op)
}
