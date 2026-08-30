package async_circular_buffer

import "base:runtime"
import "core:bytes"
import "core:testing"

Circular_Buffer :: struct {
	buf:  []u8,
	wc:   int,
	rc:   int,
	wa:   int,
	ra:   int,
	mask: int,
}

create :: proc(buf: []u8) -> Circular_Buffer {
	assert(runtime.is_power_of_two(len(buf)))
	return {buf = buf, wa = len(buf), mask = len(buf) - 1}
}

can_write :: #force_inline proc(self: ^Circular_Buffer, n: int) -> bool {
	return self.wa >= n
}

can_read :: #force_inline proc(self: ^Circular_Buffer, n: int) -> bool {
	return self.ra >= n
}

@(private = "file")
advance_writer :: #force_inline proc(self: ^Circular_Buffer, n: int) {
	self.wc = (self.wc + n) & self.mask
	self.ra += n
	self.wa -= n
}

@(private = "file")
advance_reader :: #force_inline proc(self: ^Circular_Buffer, n: int) {
	self.rc = (self.rc + n) & self.mask
	self.ra -= n
	self.wa += n
}

write :: proc(self: ^Circular_Buffer, buf: []u8) -> (ok: bool) {
	n := len(buf)
	if self.wa < n do return false

	#no_bounds_check {
		if self.wc + n > len(self.buf) {
			first_chunk_len := len(self.buf) - self.wc
			copy_slice(self.buf[self.wc:], buf[:first_chunk_len])
			copy_slice(self.buf, buf[first_chunk_len:])
		} else {
			copy_slice(self.buf[self.wc:], buf)
		}
	}

	advance_writer(self, n)
	return true
}

read :: proc(self: ^Circular_Buffer, dst: []u8) -> (ok: bool) {
	n := len(dst)
	if self.ra < n do return false

	#no_bounds_check {
		if self.rc + n > len(self.buf) {
			first_chunk_len := len(self.buf) - self.rc
			copy_slice(dst, self.buf[self.rc:])
			copy_slice(dst[first_chunk_len:], self.buf[:n - first_chunk_len])
		} else {
			copy_slice(dst, self.buf[self.rc:self.rc + n])
		}
	}

	advance_reader(self, n)
	return true
}

index_of :: proc {
	index_of_byte,
	index_of_bytes,
}

index_of_byte :: proc(self: ^Circular_Buffer, b: u8) -> int {
	return index_of_byte_at(self, b, 0)
}

index_of_bytes :: proc(self: ^Circular_Buffer, needle: []u8) -> int {
	n := len(needle)
	if n == 0 || self.ra < n do return -1
	if n == 1 do return index_of(self, needle[0])

	offset := 0
	max_offset := self.ra - n

	#no_bounds_check {
		for offset <= max_offset {
			offset = index_of_byte_at(self, needle[0], offset)
			if offset == -1 || offset > max_offset do return -1

			match := true
			for j in 1 ..< n {
				idx := (self.rc + offset + j) & self.mask
				if self.buf[idx] != needle[j] {
					match = false
					break
				}
			}

			if match do return offset
			offset += 1
		}
	}

	return -1
}

@(private = "file")
index_of_byte_at :: proc(self: ^Circular_Buffer, b: u8, offset: int) -> int {
	available := self.ra - offset
	if available <= 0 do return -1

	start_rc := (self.rc + offset) & self.mask
	#no_bounds_check {
		first_len := min(available, len(self.buf) - start_rc)
		if idx := bytes.index_byte(self.buf[start_rc:start_rc + first_len], b); idx != -1 {
			return offset + idx
		}

		second_len := available - first_len
		if second_len > 0 {
			if idx := bytes.index_byte(self.buf[:second_len], b); idx != -1 {
				return offset + first_len + idx
			}
		}
	}

	return -1
}

peek_read :: proc(self: ^Circular_Buffer) -> []u8 {
	n := min(self.ra, len(self.buf) - self.rc)
	#no_bounds_check {
		return self.buf[self.rc:self.rc + n]
	}
}

commit_read :: proc(self: ^Circular_Buffer, n: int) -> (ok: bool) {
	if n < 0 || n > self.ra do return false
	advance_reader(self, n)
	return true
}

peek_write :: proc(self: ^Circular_Buffer) -> []u8 {
	n := min(self.wa, len(self.buf) - self.wc)
	#no_bounds_check {
		return self.buf[self.wc:self.wc + n]
	}
}

commit_write :: proc(self: ^Circular_Buffer, n: int) -> (ok: bool) {
	if n < 0 || n > self.wa do return false
	advance_writer(self, n)
	return true
}

is_empty :: #force_inline proc(self: ^Circular_Buffer) -> bool {
	return self.ra == 0
}

clear :: proc(self: ^Circular_Buffer) {
	self.wc = 0
	self.rc = 0
	self.wa = len(self.buf)
	self.ra = 0
}

@(test)
test_init_deinit :: proc(t: ^testing.T) {
	cb_buf: [16]u8
	cb := create(cb_buf[:])

	testing.expect_value(t, len(cb.buf), 16)
	testing.expect_value(t, cb.wa, 16)
	testing.expect_value(t, cb.ra, 0)
	testing.expect_value(t, cb.wc, 0)
	testing.expect_value(t, cb.rc, 0)
}

@(test)
test_write_and_read_basic :: proc(t: ^testing.T) {
	cb_buf: [16]u8
	cb := create(cb_buf[:])

	data := []u8{10, 20, 30, 40, 50}
	ok_w := write(&cb, data)
	testing.expect(t, ok_w)
	testing.expect_value(t, cb.ra, 5)
	testing.expect_value(t, cb.wa, 11)

	dst: [5]u8
	ok_r := read(&cb, dst[:])
	testing.expect(t, ok_r)
	testing.expect_value(t, dst[0], 10)
	testing.expect_value(t, dst[4], 50)
	testing.expect_value(t, cb.ra, 0)
	testing.expect_value(t, cb.wa, 16)
}

@(test)
test_write_wrap_around :: proc(t: ^testing.T) {
	cb_buf: [8]u8
	cb := create(cb_buf[:])

	write(&cb, []u8{1, 2, 3, 4, 5, 6})
	dst: [6]u8
	read(&cb, dst[:])

	testing.expect_value(t, cb.wc, 6)

	ok_w := write(&cb, []u8{10, 20, 30, 40, 50, 60})
	testing.expect(t, ok_w)
	testing.expect_value(t, cb.wc, 4)
	testing.expect_value(t, cb.ra, 6)

	testing.expect_value(t, cb.buf[6], 10)
	testing.expect_value(t, cb.buf[7], 20)
	testing.expect_value(t, cb.buf[0], 30)
	testing.expect_value(t, cb.buf[1], 40)
	testing.expect_value(t, cb.buf[2], 50)
	testing.expect_value(t, cb.buf[3], 60)
}

@(test)
test_read_wrap_around :: proc(t: ^testing.T) {
	cb_buf: [8]u8
	cb := create(cb_buf[:])

	write(&cb, []u8{1, 2, 3, 4, 5, 6})
	dst_tmp: [6]u8
	read(&cb, dst_tmp[:])

	write(&cb, []u8{100, 101, 102, 103, 104, 105})

	dst: [6]u8
	ok_r := read(&cb, dst[:])
	testing.expect(t, ok_r)

	testing.expect_value(t, dst[0], 100)
	testing.expect_value(t, dst[1], 101)
	testing.expect_value(t, dst[2], 102)
	testing.expect_value(t, dst[3], 103)
	testing.expect_value(t, dst[4], 104)
	testing.expect_value(t, dst[5], 105)

	testing.expect_value(t, cb.rc, 4)
	testing.expect_value(t, cb.ra, 0)
}

@(test)
test_bounds_and_overflow :: proc(t: ^testing.T) {
	cb_buf: [4]u8
	cb := create(cb_buf[:])

	ok_w := write(&cb, []u8{1, 2, 3, 4, 5})
	testing.expect(t, !ok_w)

	write(&cb, []u8{1, 2, 3, 4})
	testing.expect_value(t, cb.wa, 0)

	ok_w_full := write(&cb, []u8{5})
	testing.expect(t, !ok_w_full)

	dst: [5]u8
	ok_r := read(&cb, dst[:])
	testing.expect(t, !ok_r)
}

@(test)
test_continuous_stream_stress :: proc(t: ^testing.T) {
	cb_buf: [8]u8
	cb := create(cb_buf[:])

	dst: [3]u8
	counter: u8 = 1

	for _ in 0 ..< 100 {
		w_data := []u8{counter, counter + 1, counter + 2}
		ok_w := write(&cb, w_data)
		testing.expect(t, ok_w)

		ok_r := read(&cb, dst[:])
		testing.expect(t, ok_r)

		testing.expect_value(t, dst[0], counter)
		testing.expect_value(t, dst[1], counter + 1)
		testing.expect_value(t, dst[2], counter + 2)

		counter += 3
	}

	testing.expect_value(t, cb.ra, 0)
	testing.expect_value(t, cb.wa, 8)
}

@(test)
test_peek_and_discard :: proc(t: ^testing.T) {
	cb_buf: [8]u8
	cb := create(cb_buf[:])

	write(&cb, []u8{10, 20, 30, 40, 50, 60})

	chunk := peek_read(&cb)
	testing.expect_value(t, len(chunk), 6)
	testing.expect_value(t, chunk[0], 10)

	ok_d := commit_read(&cb, 2)
	testing.expect(t, ok_d)
	testing.expect_value(t, cb.ra, 4)

	chunk2 := peek_read(&cb)
	testing.expect_value(t, len(chunk2), 4)
	testing.expect_value(t, chunk2[0], 30)

	commit_read(&cb, 4)
	testing.expect_value(t, cb.ra, 0)
	testing.expect_value(t, cb.wa, 8)
}

@(test)
test_peek_discard_wrap_around :: proc(t: ^testing.T) {
	cb_buf: [8]u8
	cb := create(cb_buf[:])

	write(&cb, []u8{10, 20, 30, 40, 50, 60})
	commit_read(&cb, 5)

	testing.expect_value(t, cb.rc, 5)
	testing.expect_value(t, cb.ra, 1)

	write(&cb, []u8{70, 80, 90, 100, 110})
	testing.expect_value(t, cb.ra, 6)

	chunk1 := peek_read(&cb)
	testing.expect_value(t, len(chunk1), 3)
	testing.expect_value(t, chunk1[0], 60)
	testing.expect_value(t, chunk1[1], 70)
	testing.expect_value(t, chunk1[2], 80)

	commit_read(&cb, 3)
	testing.expect_value(t, cb.rc, 0)
	testing.expect_value(t, cb.ra, 3)

	chunk2 := peek_read(&cb)
	testing.expect_value(t, len(chunk2), 3)
	testing.expect_value(t, chunk2[0], 90)
	testing.expect_value(t, chunk2[1], 100)
	testing.expect_value(t, chunk2[2], 110)

	commit_read(&cb, 3)
	testing.expect_value(t, cb.ra, 0)
	testing.expect_value(t, cb.wa, 8)
}

@(test)
test_peek_commit_write_wrap_around :: proc(t: ^testing.T) {
	cb_buf: [8]u8
	cb := create(cb_buf[:])

	write(&cb, []u8{10, 20, 30, 40, 50})
	commit_read(&cb, 4)

	testing.expect_value(t, cb.wa, 7)
	testing.expect_value(t, cb.wc, 5)
	testing.expect_value(t, cb.rc, 4)
	testing.expect_value(t, cb.ra, 1)

	chunk1 := peek_write(&cb)
	testing.expect_value(t, len(chunk1), 3)

	chunk1[0] = 60
	chunk1[1] = 70
	chunk1[2] = 80
	commit_write(&cb, 3)

	testing.expect_value(t, cb.wc, 0)
	testing.expect_value(t, cb.ra, 4)

	chunk2 := peek_write(&cb)
	testing.expect_value(t, len(chunk2), 4)

	chunk2[0] = 90
	chunk2[1] = 100
	chunk2[2] = 110
	commit_write(&cb, 3)

	testing.expect_value(t, cb.wc, 3)
	testing.expect_value(t, cb.ra, 7)
}

@(test)
test_index_of_basic :: proc(t: ^testing.T) {
	cb_buf: [16]u8
	cb := create(cb_buf[:])

	testing.expect_value(t, index_of(&cb, 'A'), -1)

	write(&cb, []u8{'H', 'E', 'L', 'L', 'O'})
	testing.expect_value(t, index_of(&cb, 'H'), 0)
	testing.expect_value(t, index_of(&cb, 'L'), 2)
	testing.expect_value(t, index_of(&cb, 'O'), 4)
	testing.expect_value(t, index_of(&cb, 'X'), -1)
}

@(test)
test_index_of_wrap_around :: proc(t: ^testing.T) {
	cb_buf: [8]u8
	cb := create(cb_buf[:])

	write(&cb, []u8{1, 2, 3, 4, 5, 6})
	commit_read(&cb, 5)

	write(&cb, []u8{'A', '\n', 'B', 'C', 'D', 'E'})

	testing.expect_value(t, index_of(&cb, 'A'), 1)
	testing.expect_value(t, index_of(&cb, '\n'), 2)
	testing.expect_value(t, index_of(&cb, 'B'), 3)
	testing.expect_value(t, index_of(&cb, 'E'), 6)
	testing.expect_value(t, index_of(&cb, 'Z'), -1)
}

@(test)
test_index_of_exact_edge_cases :: proc(t: ^testing.T) {
	cb_buf: [8]u8
	cb := create(cb_buf[:])

	write(&cb, []u8{10, 20, 30, 40, 50})
	commit_read(&cb, 4)

	testing.expect_value(t, index_of(&cb, 50), 0)

	write(&cb, []u8{99})
	testing.expect_value(t, index_of(&cb, 99), 1)
}

@(test)
test_index_of_bytes_basic :: proc(t: ^testing.T) {
	cb_buf: [16]u8
	cb := create(cb_buf[:])

	write(&cb, transmute([]u8)string("GET / HTTP/1.1\r\n"))

	idx := index_of_bytes(&cb, transmute([]u8)string("\r\n"))
	testing.expect_value(t, idx, 14)

	idx_http := index_of_bytes(&cb, transmute([]u8)string("HTTP"))
	testing.expect_value(t, idx_http, 6)

	idx_notFound := index_of_bytes(&cb, transmute([]u8)string("POST"))
	testing.expect_value(t, idx_notFound, -1)
}

@(test)
test_index_of_bytes_wrap_around :: proc(t: ^testing.T) {
	cb_buf: [16]u8
	cb := create(cb_buf[:])

	write(&cb, transmute([]u8)string("12345678901234"))
	commit_read(&cb, 14)

	write(&cb, transmute([]u8)string("HEAD\r\n"))

	testing.expect_value(t, cb.rc, 14)
	testing.expect_value(t, cb.ra, 6)

	testing.expect_value(t, index_of_bytes(&cb, transmute([]u8)string("HEAD")), 0)
	testing.expect_value(t, index_of_bytes(&cb, transmute([]u8)string("AD\r")), 2)
	testing.expect_value(t, index_of_bytes(&cb, transmute([]u8)string("\r\n")), 4)
	testing.expect_value(t, index_of_bytes(&cb, transmute([]u8)string("EA")), 1)
}

