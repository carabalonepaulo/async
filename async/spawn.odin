package async

import "base:runtime"
import "core:container/queue"

import "coro"

spawn_without_data :: proc(
	fn: proc(),
	stack_size: uint = DEFAULT_STACK_SIZE,
	storage_size: uint = DEFAULT_STORAGE_SIZE,
	stack_allocator := context.allocator,
) -> Handle {
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
	assert(size_of(A) <= DEFAULT_STORAGE_SIZE, "storage is too small for spawn arguments")

	a := a

	ud := create_ud(rawptr(fn))
	raw_fn := proc "c" (co: ^coro.Coro) {
		ud := (^Internal_State)(coro.get_user_data(co))
		context = ud.ctx

		a: A
		coro.pop(ud.co, &a, size_of(A))

		((proc(a: A))(ud.fn))(a)
		call_hook(ud, .Exit)
	}

	desc := create_desc(raw_fn, ud)
	coro.check(coro.create(&ud.co, &desc))
	coro.push(ud.co, &a, size_of(a))

	queue.enqueue(&scheduler.ready, ud.id)
	return Handle(ud.id)
}

spawn_with_poly2 :: proc(a: $A, b: $B, fn: proc(a: A, b: B)) -> Handle {
	assert(
		size_of(A) + size_of(B) <= DEFAULT_STORAGE_SIZE,
		"storage is too small for spawn arguments",
	)

	a := a
	b := b

	ud := create_ud(rawptr(fn))
	raw_fn := proc "c" (co: ^coro.Coro) {
		ud := (^Internal_State)(coro.get_user_data(co))
		context = ud.ctx

		b: B
		a: A
		coro.pop(ud.co, &b, size_of(B))
		coro.pop(ud.co, &a, size_of(A))

		((proc(a: A, b: B))(ud.fn))(a, b)
		call_hook(ud, .Exit)
	}

	desc := create_desc(raw_fn, ud)
	coro.check(coro.create(&ud.co, &desc))
	coro.push(ud.co, &a, size_of(a))
	coro.push(ud.co, &b, size_of(b))

	queue.enqueue(&scheduler.ready, ud.id)
	return Handle(ud.id)
}

spawn_with_poly3 :: proc(a: $A, b: $B, c: $C, fn: proc(a: A, b: B, c: C)) -> Handle {
	assert(
		size_of(A) + size_of(B) + size_of(C) <= DEFAULT_STORAGE_SIZE,
		"storage is too small for spawn arguments",
	)

	a := a
	b := b
	c := c

	ud := create_ud(rawptr(fn))
	raw_fn := proc "c" (co: ^coro.Coro) {
		ud := (^Internal_State)(coro.get_user_data(co))
		context = ud.ctx

		c: C
		b: B
		a: A

		coro.pop(ud.co, &c, size_of(C))
		coro.pop(ud.co, &b, size_of(B))
		coro.pop(ud.co, &a, size_of(A))

		((proc(a: A, b: B, c: C))(ud.fn))(a, b, c)
		call_hook(ud, .Exit)
	}

	desc := create_desc(raw_fn, ud)
	coro.check(coro.create(&ud.co, &desc))
	coro.push(ud.co, &a, size_of(a))
	coro.push(ud.co, &b, size_of(b))
	coro.push(ud.co, &c, size_of(c))

	queue.enqueue(&scheduler.ready, ud.id)
	return Handle(ud.id)
}

spawn_with_poly4 :: proc(a: $A, b: $B, c: $C, d: $D, fn: proc(a: A, b: B, c: C, d: D)) -> Handle {
	assert(
		size_of(A) + size_of(B) + size_of(C) + size_of(D) <= DEFAULT_STORAGE_SIZE,
		"storage is too small for spawn arguments",
	)

	a := a
	b := b
	c := c
	d := d

	ud := create_ud(rawptr(fn))
	raw_fn := proc "c" (co: ^coro.Coro) {
		ud := (^Internal_State)(coro.get_user_data(co))
		context = ud.ctx

		d: D
		c: C
		b: B
		a: A

		coro.pop(ud.co, &d, size_of(D))
		coro.pop(ud.co, &c, size_of(C))
		coro.pop(ud.co, &b, size_of(B))
		coro.pop(ud.co, &a, size_of(A))

		((proc(a: A, b: B, c: C, d: D))(ud.fn))(a, b, c, d)
		call_hook(ud, .Exit)
	}

	desc := create_desc(raw_fn, ud)
	coro.check(coro.create(&ud.co, &desc))
	coro.push(ud.co, &a, size_of(a))
	coro.push(ud.co, &b, size_of(b))
	coro.push(ud.co, &c, size_of(c))
	coro.push(ud.co, &d, size_of(d))

	queue.enqueue(&scheduler.ready, ud.id)
	return Handle(ud.id)
}

spawn_with_poly5 :: proc(
	a: $A,
	b: $B,
	c: $C,
	d: $D,
	e: $E,
	fn: proc(a: A, b: B, c: C, d: D, e: E),
) -> Handle {
	assert(
		size_of(A) + size_of(B) + size_of(C) + size_of(D) + size_of(E) <= DEFAULT_STORAGE_SIZE,
		"storage is too small for spawn arguments",
	)

	a := a
	b := b
	c := c
	d := d
	e := e

	ud := create_ud(rawptr(fn))
	raw_fn := proc "c" (co: ^coro.Coro) {
		ud := (^Internal_State)(coro.get_user_data(co))
		context = ud.ctx

		e: E
		d: D
		c: C
		b: B
		a: A

		coro.pop(ud.co, &e, size_of(E))
		coro.pop(ud.co, &d, size_of(D))
		coro.pop(ud.co, &c, size_of(C))
		coro.pop(ud.co, &b, size_of(B))
		coro.pop(ud.co, &a, size_of(A))

		((proc(a: A, b: B, c: C, d: D, e: E))(ud.fn))(a, b, c, d, e)
		call_hook(ud, .Exit)
	}

	desc := create_desc(raw_fn, ud)
	coro.check(coro.create(&ud.co, &desc))
	coro.push(ud.co, &a, size_of(a))
	coro.push(ud.co, &b, size_of(b))
	coro.push(ud.co, &c, size_of(c))
	coro.push(ud.co, &d, size_of(d))
	coro.push(ud.co, &e, size_of(e))

	queue.enqueue(&scheduler.ready, ud.id)
	return Handle(ud.id)
}

