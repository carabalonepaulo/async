package async

import "base:runtime"
import "core:container/queue"

import "coro"

spawn_without_data :: proc(fn: proc()) -> Handle {
	ud := create_ud(rawptr(fn))
	raw_fn := proc "c" (co: ^coro.Coro) {
		ud := (^Internal_State)(coro.get_user_data(co))
		context = ud.ctx
		((proc())(ud.fn))()
		call_hook(ud, .Exit)
	}
	desc := create_desc(raw_fn, ud)
	coro.check(coro.create(&ud.co, &desc))
	queue.enqueue(&scheduler.ready, ud.id)
	return Handle(ud.id)
}

spawn_with_poly :: proc(a: $A, fn: proc(a: A)) -> Handle {
	raw_fn := proc "c" (co: ^coro.Coro) {
		ud := (^Internal_State)(coro.get_user_data(co))
		context = ud.ctx

		os := transmute(One_Shot(A))(ud.args)
		a := recv(os)

		((proc(a: A))(ud.fn))(a)
		call_hook(ud, .Exit)
	}
	return create_and_start(a, rawptr(fn), raw_fn)
}

spawn_with_poly2 :: proc(a: $A, b: $B, fn: proc(a: A, b: B)) -> Handle {
	Args :: struct {
		a: A,
		b: B,
	}

	raw_fn := proc "c" (co: ^coro.Coro) {
		ud := (^Internal_State)(coro.get_user_data(co))
		context = ud.ctx

		os := transmute(One_Shot(Args))(ud.args)
		args := recv(os)

		((proc(a: A, b: B))(ud.fn))(args.a, args.b)
		call_hook(ud, .Exit)
	}
	return create_and_start(Args{a, b}, rawptr(fn), raw_fn)
}

spawn_with_poly3 :: proc(a: $A, b: $B, c: $C, fn: proc(a: A, b: B, c: C)) -> Handle {
	Args :: struct {
		a: A,
		b: B,
		c: C,
	}

	raw_fn := proc "c" (co: ^coro.Coro) {
		ud := (^Internal_State)(coro.get_user_data(co))
		context = ud.ctx

		os := transmute(One_Shot(Args))(ud.args)
		args := recv(os)

		((proc(a: A, b: B, c: C))(ud.fn))(args.a, args.b, args.c)
		call_hook(ud, .Exit)
	}
	return create_and_start(Args{a, b, c}, rawptr(fn), raw_fn)
}

spawn_with_poly4 :: proc(a: $A, b: $B, c: $C, d: $D, fn: proc(a: A, b: B, c: C, d: D)) -> Handle {
	Args :: struct {
		a: A,
		b: B,
		c: C,
		d: D,
	}

	raw_fn := proc "c" (co: ^coro.Coro) {
		ud := (^Internal_State)(coro.get_user_data(co))
		context = ud.ctx

		os := transmute(One_Shot(Args))(ud.args)
		args := recv(os)

		((proc(a: A, b: B, c: C, d: D))(ud.fn))(args.a, args.b, args.c, args.d)
		call_hook(ud, .Exit)
	}
	return create_and_start(Args{a, b, c, d}, rawptr(fn), raw_fn)
}

spawn_with_poly5 :: proc(
	a: $A,
	b: $B,
	c: $C,
	d: $D,
	e: $E,
	fn: proc(a: A, b: B, c: C, d: D, e: E),
) -> Handle {
	Args :: struct {
		a: A,
		b: B,
		c: C,
		d: D,
		e: E,
	}

	raw_fn := proc "c" (co: ^coro.Coro) {
		ud := (^Internal_State)(coro.get_user_data(co))
		context = ud.ctx

		os := transmute(One_Shot(Args))(ud.args)
		args := recv(os)

		((proc(a: A, b: B, c: C, d: D, e: E))(ud.fn))(args.a, args.b, args.c, args.d, args.e)
		call_hook(ud, .Exit)
	}
	return create_and_start(Args{a, b, c, d, e}, rawptr(fn), raw_fn)
}

@(private = "file")
create_and_start :: proc(args: $A, fn: rawptr, raw_fn: proc "c" (co: ^coro.Coro)) -> Handle {
	os := create_one_shot(A)
	send(os, args)

	ud := create_ud(fn, transmute(u64)(os))
	desc := create_desc(raw_fn, ud)
	coro.check(coro.create(&ud.co, &desc))

	queue.enqueue(&scheduler.ready, ud.id)
	return Handle(ud.id)
}

