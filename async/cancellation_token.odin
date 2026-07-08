package async

import "core:time"

Cancellation_Token :: distinct Chan(Empty)

create_cancel_token :: proc() -> Cancellation_Token {
	self := (Cancellation_Token)(create_chan(Empty, 0))
	add_cancel_token(self.id)
	return self
}

cancel_token_branch :: proc(self: Cancellation_Token) -> Case {
	return default_branch((Chan(Empty))(self))
}

trigger :: proc(self: Cancellation_Token) {
	remove_cancel_token(self.id)
	chan_destroy((Chan(Empty))(self))
}

is_triggered :: #force_inline proc(self: Cancellation_Token) -> bool {
	return !is_chan_alive_by_id(self.id)
}

cancel_after :: proc(self: Cancellation_Token, duration: time.Duration) {
	timer(duration, proc(ud: rawptr) {
			id := transmute(u64)(ud)
			chan := Chan(Empty) {
				id = id,
			}
			token := (Cancellation_Token)(chan)
			trigger(token)
		}, transmute(rawptr)(self.id))
}

wait :: proc(self: Cancellation_Token) {
	select({branch(self)})
}

@(private)
add_cancel_token :: proc(id: u64) {
	sched := get_scheduler()
	sched.active_cancel_tokens[id] = true
}

@(private)
remove_cancel_token :: proc(id: u64) {
	sched := get_scheduler()
	delete_key(&sched.active_cancel_tokens, id)
}

@(private)
destroy_cancel_tokens :: proc() {
	sched := get_scheduler()
	for id in sched.active_cancel_tokens {
		chan := (Cancellation_Token)(Chan(Empty){id = id})
		trigger(chan)
	}
	delete(sched.active_cancel_tokens)
}

