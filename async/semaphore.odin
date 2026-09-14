package async

Semaphore :: distinct Chan(Empty)

create_semaphore :: proc(n: int) -> Semaphore {
	assert(n > 0, "semaphore count must be positive")
	self := create_chan(Empty, n)
	for _ in 0 ..< n do chan_send(self, Empty{})
	return Semaphore(self)
}

semaphore_destroy :: proc(self: Semaphore) {
	chan := (Chan(Empty))(self)
	clear(chan)
	destroy(chan)
}

try_acquire :: proc(self: Semaphore) -> bool {
	_, ok := try_recv((Chan(Empty))(self))
	return ok
}

acquire :: proc(self: Semaphore, cancel: Maybe(Cancellation_Token) = nil) -> (ok: bool) {
	chan := (Chan(Empty))(self)
	if cancel, cancel_ok := cancel.(Cancellation_Token); cancel_ok {
		idx := select({branch(cancel), branch(chan, nil, &ok)})
		if idx == 0 do return false
	} else do select({branch(chan, nil, &ok)})
	return
}

release :: proc(self: Semaphore) {
	chan := (Chan(Empty))(self)
	inner := get_inner(chan)
	assert(inner.items.len < cap(inner.items.data))
	chan_send(chan, Empty{})
}

@(deferred_in_out = _guard)
guard :: proc(self: Semaphore, cancel: Maybe(Cancellation_Token) = nil) -> bool {
	return acquire(self, cancel)
}

@(private = "file")
_guard :: proc(self: Semaphore, _: Maybe(Cancellation_Token), ok: bool) {
	if ok do release(self)
}

