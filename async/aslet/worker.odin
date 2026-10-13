package async_aslet

import "base:intrinsics"
import "base:runtime"
import "core:container/pool"
import "core:mem"
import "core:sync/chan"
import "core:thread"

import ".."

DEFAULT_CAPACITY :: 4096

Worker :: struct {
	th:    ^thread.Thread,
	in_ch: chan.Chan(^Operation),
	pool:  pool.Pool(Operation),
}

init :: proc(self: ^Worker, cap: int = DEFAULT_CAPACITY) -> (err: mem.Allocator_Error) {
	input := chan.create_buffered(chan.Chan(^Operation), cap, context.allocator) or_return
	defer if err != nil do chan.destroy(&input)

	op_pool: pool.Pool(Operation)
	pool.init(&op_pool, "_link") or_return
	defer if err != nil do pool.destroy(&op_pool)

	self.in_ch = input
	self.th = thread.create_and_start_with_poly_data(input, run)
	self.pool = op_pool

	return .None
}

deinit :: proc(self: ^Worker) {
	assert(pool.num_outstanding(&self.pool) == 0)

	chan.close(self.in_ch)
	thread.destroy(self.th)
	chan.destroy(self.in_ch)

	pool.destroy(&self.pool)
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

	consumer = new(Consumer, runtime.default_allocator()) or_return
	ref := async.add_ref(consumer)
	consumer^ = Consumer{ref.id, &self.pool, self.in_ch, out_ch, 0}

	async.schedule(ref, proc(ref: async.Ref(Consumer)) -> bool {
		consumer := async.try_get_ref(ref) or_return
		poll(consumer)
		return true
	})

	return consumer, .None
}

@(private)
run :: proc(input_ch: chan.Chan(^Operation)) {
	for {
		op := chan.recv(input_ch) or_break

		_, ok := intrinsics.atomic_compare_exchange_strong(&op.state, .Pending, .Claimed)
		if !ok {
			chan.send(op.out_ch, op)
			continue
		}

		op.run(&op.specifics)
		chan.send(op.out_ch, op)
	}
}
