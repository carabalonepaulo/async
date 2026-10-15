package async

import "base:builtin"
import "core:testing"

import il "index_list"
import "storage"

@(private = "file")
Inner :: struct($T: typeid) {
	idx:       int,
	items:     []T,
	receivers: il.Index_List(u64),
	drop:      proc(value: ^T),
}

Broadcaster :: struct($T: typeid) {
	id:      u64,
	_marker: [0]T,
}

create_broadcaster :: proc($T: typeid, cap: int, drop: proc(value: ^T) = nil) -> Broadcaster(T) {
	assert(cap > 0, "broadcaster capacity must be greater than zero")

	res := Resource {
		id = auto_cast Internal_Resource.Broadcaster,
	}
	sender := load_inline(&res.ud, Inner(T))
	sender.items = make([]T, cap)
	sender.drop = drop
	il.init(&sender.receivers)

	id := add_resource(res)
	return Broadcaster(T){id = id}
}

destroy_broadcaster :: proc(self: Broadcaster($T)) {
	res, ok := try_remove_resource(self.id)
	assert(ok, "invalid broadcaster")

	sender := load_inline(&res.ud, Inner(T))

	if sender.drop != nil {
		cap := builtin.len(sender.items)
		valid := min(sender.idx, cap)
		idx := sender.idx - valid
		for i in idx ..< sender.idx do sender.drop(&sender.items[i % cap])
	}

	it := il.iter(&sender.receivers)
	for _, raw_id in il.iterate(&it) {
		receiver := Broadcaster_Receiver(T) {
			id = raw_id^,
		}
		unsubscribe(receiver)
	}
	il.deinit(&sender.receivers)

	delete(sender.items)
}

subscribe :: proc(self: Broadcaster($T)) -> Broadcaster_Receiver(T) {
	sender := get_sender(self)

	res := Resource{}
	receiver := load_inline(&res.ud, Inner_Receiver(T))
	receiver.idx = sender.idx
	receiver.sender = sender

	entry := storage.entry(&scheduler.resources)
	receiver.id = il.add(&sender.receivers, storage.get_id(&entry))

	id := storage.insert(&entry, res)
	return Broadcaster_Receiver(T){id = id}
}

broadcaster_send :: proc(self: Broadcaster($T), value: T) {
	sender := get_sender(self)
	cap := builtin.len(sender.items)
	idx := sender.idx % cap

	if sender.idx >= cap && sender.drop != nil {
		sender.drop(&sender.items[idx])
	}

	sender.items[idx] = value
	sender.idx += 1

	it := il.iter(&sender.receivers)
	for _, raw_id in il.iterate(&it) {
		receiver := get_receiver(transmute(Broadcaster_Receiver(T))(raw_id^))
		handle := receiver.waiter.(Handle) or_continue
		wake_case(handle, receiver.case_idx, true)
		receiver.waiter = nil
		receiver.case_idx = 0
	}
}

@(private = "file")
Inner_Receiver :: struct($T: typeid) {
	id:       il.Id,
	idx:      int,
	sender:   ^Inner(T),
	waiter:   Maybe(Handle),
	case_idx: int,
}

Broadcaster_Receiver :: struct($T: typeid) {
	id:      u64,
	_marker: [0]T,
}

unsubscribe :: proc(self: Broadcaster_Receiver($T)) {
	res, ok := try_remove_resource(self.id)
	assert(ok, "invalid broadcaster receiver")

	inner := load_inline(&res.ud, Inner_Receiver(T))
	if handle, ok := inner.waiter.(Handle); ok {
		wake_waiter(handle, inner.case_idx, false)
	}
	il.remove(&inner.sender.receivers, inner.id)
}

broadcaster_try_recv :: proc(self: Broadcaster_Receiver($T)) -> (value: T, missed: int, ok: bool) {
	receiver := try_get_receiver(self) or_return
	return try_recv(receiver)
}

broadcaster_recv :: proc(self: Broadcaster_Receiver($T)) -> (value: T, missed: int, ok: bool) {
	receiver := try_get_receiver(self) or_return

	value, missed, ok = try_recv(receiver)
	if ok == true do return

	receiver.waiter = get_handle()
	receiver.case_idx = -1

	yield()

	receiver = try_get_receiver(self) or_return
	receiver.waiter = nil
	receiver.case_idx = 0

	return try_recv(receiver)
}

@(private = "file")
try_recv :: proc(receiver: ^Inner_Receiver($T)) -> (value: T, missed: int, ok: bool) {
	sender := receiver.sender
	cap := builtin.len(sender.items)

	if receiver.idx == sender.idx do return {}, 0, false

	lag := sender.idx - receiver.idx
	if lag > cap {
		missed = lag - cap
		receiver.idx = sender.idx - cap
	}

	value = sender.items[receiver.idx % cap]
	receiver.idx += 1
	ok = true

	return
}

@(private = "file")
wrap_add :: proc "contextless" (value: ^int, n: int, limit: int) {
	value^ += n
	if value^ >= limit do value^ -= limit
}

@(private = "file")
try_get_sender :: proc(self: Broadcaster($T)) -> (inner: ^Inner(T), ok: bool) {
	res := try_get_resource(self.id) or_return
	return load_inline(&res.ud, Inner(T)), true
}

@(private = "file")
get_sender :: proc(self: Broadcaster($T)) -> ^Inner(T) {
	inner, ok := try_get_sender(self)
	assert(ok, "invalid broadcaster")
	return inner
}

@(private = "file")
try_get_receiver :: proc(self: Broadcaster_Receiver($T)) -> (inner: ^Inner_Receiver(T), ok: bool) {
	res := try_get_resource(self.id) or_return
	return load_inline(&res.ud, Inner_Receiver(T)), true
}

@(private = "file")
get_receiver :: proc(self: Broadcaster_Receiver($T)) -> ^Inner_Receiver(T) {
	inner, ok := try_get_receiver(self)
	assert(ok, "invalid broadcaster")
	return inner
}

@(test)
test_broadcaster_send_recv_ok :: proc(t: ^testing.T) {
	init()
	defer deinit()

	sx := create_broadcaster(int, 5)
	defer destroy_broadcaster(sx)

	rx := subscribe(sx)
	broadcaster_send(sx, 10)

	A :: proc(t: ^testing.T, rx: Broadcaster_Receiver(int)) {
		value, missed, ok := broadcaster_recv(rx)
		testing.expect(t, ok)
		testing.expect(t, value == 10)
		testing.expect(t, missed == 0)
	}
	block(spawn(t, rx, A))
}

@(test)
test_broadcaster_lag_overflow :: proc(t: ^testing.T) {
	init()
	defer deinit()

	A :: proc(t: ^testing.T) {
		sx := create_broadcaster(int, 2)
		defer destroy_broadcaster(sx)

		rx := subscribe(sx)

		broadcaster_send(sx, 100)
		broadcaster_send(sx, 200)
		broadcaster_send(sx, 300)
		broadcaster_send(sx, 400)

		val, missed, ok := broadcaster_recv(rx)
		testing.expect(t, ok)
		testing.expect(t, val == 300)
		testing.expect(t, missed == 2)

		val, missed, ok = broadcaster_recv(rx)
		testing.expect(t, ok)
		testing.expect(t, val == 400)
	}
	block(spawn(t, A))
}

@(test)
test_broadcaster_fan_out :: proc(t: ^testing.T) {
	init()
	defer deinit()

	A :: proc(t: ^testing.T) {
		sx := create_broadcaster(int, 5)
		defer destroy_broadcaster(sx)

		rx1 := subscribe(sx)
		rx2 := subscribe(sx)
		rx3 := subscribe(sx)

		broadcaster_send(sx, 10)
		broadcaster_send(sx, 20)

		Worker :: proc(t: ^testing.T, rx: Broadcaster_Receiver(int)) {
			val1, missed1, ok1 := broadcaster_recv(rx)
			testing.expect(t, ok1, "expected ok1 = true")
			testing.expectf(t, val1 == 10, "expected val1 = 10, got %d", val1)
			testing.expectf(t, missed1 == 0, "expected missed1 = 0, got %d", missed1)

			val2, missed2, ok2 := broadcaster_recv(rx)
			testing.expect(t, ok2, "expected ok2 = true")
			testing.expectf(t, val2 == 20, "expected val2 = 20, got %d", val2)
			testing.expectf(t, missed2 == 0, "expected missed2 = 0, got %d", missed2)
		}

		w1 := spawn(t, rx1, Worker)
		w2 := spawn(t, rx2, Worker)
		w3 := spawn(t, rx3, Worker)

		block(w1)
		block(w2)
		block(w3)
	}

	block(spawn(t, A))
}

@(test)
test_broadcaster_late_join :: proc(t: ^testing.T) {
	init()
	defer deinit()

	A :: proc(t: ^testing.T) {
		sx := create_broadcaster(int, 5)
		defer destroy_broadcaster(sx)

		rx1 := subscribe(sx)

		broadcaster_send(sx, 100)
		broadcaster_send(sx, 200)

		rx2 := subscribe(sx)

		broadcaster_send(sx, 300)

		val, missed, ok := broadcaster_recv(rx1)
		testing.expect(t, ok, "rx1 expected ok = true for 100")
		testing.expectf(t, val == 100, "rx1 expected val = 100, got %d", val)
		testing.expectf(t, missed == 0, "rx1 expected missed = 0, got %d", missed)

		val, missed, ok = broadcaster_recv(rx1)
		testing.expect(t, ok, "rx1 expected ok = true for 200")
		testing.expectf(t, val == 200, "rx1 expected val = 200, got %d", val)

		val, missed, ok = broadcaster_recv(rx1)
		testing.expect(t, ok, "rx1 expected ok = true for 300")
		testing.expectf(t, val == 300, "rx1 expected val = 300, got %d", val)

		val, missed, ok = broadcaster_recv(rx2)
		testing.expect(t, ok, "rx2 expected ok = true for 300")
		testing.expectf(t, val == 300, "rx2 expected val = 300, got %d", val)
		testing.expectf(t, missed == 0, "rx2 expected missed = 0, got %d", missed)
	}

	block(spawn(t, A))
}

@(test)
test_broadcaster_exact_capacity :: proc(t: ^testing.T) {
	init()
	defer deinit()

	A :: proc(t: ^testing.T) {
		sx := create_broadcaster(int, 3)
		defer destroy_broadcaster(sx)

		rx := subscribe(sx)

		broadcaster_send(sx, 1)
		broadcaster_send(sx, 2)
		broadcaster_send(sx, 3)

		for expected in 1 ..= 3 {
			val, missed, ok := broadcaster_recv(rx)
			testing.expect(t, ok, "expected ok = true")
			testing.expectf(t, val == expected, "expected val = %d, got %d", expected, val)
			testing.expectf(t, missed == 0, "expected missed = 0, got %d", missed)
		}
	}

	block(spawn(t, A))
}

@(test)
test_broadcaster_massive_overflow :: proc(t: ^testing.T) {
	init()
	defer deinit()

	A :: proc(t: ^testing.T) {
		sx := create_broadcaster(int, 3)
		defer destroy_broadcaster(sx)

		rx := subscribe(sx)

		for i in 1 ..= 10 {
			broadcaster_send(sx, i)
		}

		val, missed, ok := broadcaster_recv(rx)
		testing.expect(t, ok, "expected ok = true")
		testing.expectf(t, val == 8, "expected val = 8, got %d", val)
		testing.expectf(t, missed == 7, "expected missed = 7, got %d", missed)

		val, missed, ok = broadcaster_recv(rx)
		testing.expect(t, ok, "expected ok = true")
		testing.expectf(t, val == 9, "expected val = 9, got %d", val)
		testing.expectf(t, missed == 0, "expected missed = 0, got %d", missed)

		val, missed, ok = broadcaster_recv(rx)
		testing.expect(t, ok, "expected ok = true")
		testing.expectf(t, val == 10, "expected val = 10, got %d", val)
		testing.expectf(t, missed == 0, "expected missed = 0, got %d", missed)
	}

	block(spawn(t, A))
}

@(test)
test_broadcaster_recv_before_send :: proc(t: ^testing.T) {
	init()
	defer deinit()

	A :: proc(t: ^testing.T) {
		sx := create_broadcaster(int, 5)
		defer destroy_broadcaster(sx)

		rx := subscribe(sx)

		Consumer :: proc(t: ^testing.T, rx: Broadcaster_Receiver(int)) {
			val, missed, ok := broadcaster_recv(rx)
			testing.expect(t, ok, "expected ok = true")
			testing.expectf(t, val == 42, "expected val = 42, got %d", val)
			testing.expectf(t, missed == 0, "expected missed = 0, got %d", missed)
		}

		cons := spawn(t, rx, Consumer)

		broadcaster_send(sx, 42)

		block(cons)
	}

	block(spawn(t, A))
}

@(test)
test_broadcaster_multiple_suspended_receivers :: proc(t: ^testing.T) {
	init()
	defer deinit()

	A :: proc(t: ^testing.T) {
		sx := create_broadcaster(int, 5)
		defer destroy_broadcaster(sx)

		rx1 := subscribe(sx)
		rx2 := subscribe(sx)

		Consumer :: proc(t: ^testing.T, rx: Broadcaster_Receiver(int)) {
			val, missed, ok := broadcaster_recv(rx)
			testing.expect(t, ok, "expected ok = true")
			testing.expectf(t, val == 99, "expected val = 99, got %d", val)
			testing.expectf(t, missed == 0, "expected missed = 0, got %d", missed)
		}

		c1 := spawn(t, rx1, Consumer)
		c2 := spawn(t, rx2, Consumer)

		broadcaster_send(sx, 99)

		block(c1)
		block(c2)
	}

	block(spawn(t, A))
}

@(test)
test_broadcaster_drop_on_overwrite :: proc(t: ^testing.T) {
	init()
	defer deinit()

	A :: proc(t: ^testing.T) {
		drop_counter := 0

		Tracked :: struct {
			counter: ^int,
		}

		custom_drop :: proc(val: ^Tracked) {
			val.counter^ += 1
		}

		sx := create_broadcaster(Tracked, 2, custom_drop)
		defer destroy_broadcaster(sx)

		_ = subscribe(sx)

		broadcaster_send(sx, Tracked{counter = &drop_counter})
		broadcaster_send(sx, Tracked{counter = &drop_counter})
		testing.expectf(t, drop_counter == 0, "expected drop_counter = 0, got %d", drop_counter)

		broadcaster_send(sx, Tracked{counter = &drop_counter})
		testing.expectf(t, drop_counter == 1, "expected drop_counter = 1, got %d", drop_counter)

		broadcaster_send(sx, Tracked{counter = &drop_counter})
		testing.expectf(t, drop_counter == 2, "expected drop_counter = 2, got %d", drop_counter)
	}

	block(spawn(t, A))
}

@(test)
test_broadcaster_drop_on_destroy :: proc(t: ^testing.T) {
	init()
	defer deinit()

	A :: proc(t: ^testing.T) {
		drop_counter := 0

		Tracked :: struct {
			counter: ^int,
		}

		custom_drop :: proc(val: ^Tracked) {
			val.counter^ += 1
		}

		sx := create_broadcaster(Tracked, 5, custom_drop)

		broadcaster_send(sx, Tracked{counter = &drop_counter})
		broadcaster_send(sx, Tracked{counter = &drop_counter})
		broadcaster_send(sx, Tracked{counter = &drop_counter})

		testing.expectf(
			t,
			drop_counter == 0,
			"expected drop_counter = 0 before destroy, got %d",
			drop_counter,
		)

		destroy_broadcaster(sx)

		testing.expectf(
			t,
			drop_counter == 3,
			"expected drop_counter = 3 after destroy (capacity 5), got %d",
			drop_counter,
		)
	}

	block(spawn(t, A))
}

@(test)
test_broadcaster_unsubscribe_suspended :: proc(t: ^testing.T) {
	init()
	defer deinit()

	A :: proc(t: ^testing.T) {
		sx := create_broadcaster(int, 5)
		defer destroy_broadcaster(sx)

		rx := subscribe(sx)

		Consumer :: proc(t: ^testing.T, rx: Broadcaster_Receiver(int)) {
			_, _, ok := broadcaster_recv(rx)
			testing.expect(t, !ok, "expected ok = false after unsubscribe wake")
		}

		c1 := spawn(t, rx, Consumer)
		unsubscribe(rx)
		block(c1)
	}

	block(spawn(t, A))
}

