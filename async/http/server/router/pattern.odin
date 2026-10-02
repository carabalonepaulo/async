package async_http_server_router_v2

import "base:builtin"
import "core:reflect"
import "core:slice"
import "core:strconv"
import "core:strings"
import "core:testing"
import "core:unicode/utf8"

ALLOWED_TYPES := []typeid{int, string}

@(private)
Param_Range :: struct {
	start: int,
	end:   int,
}

@(private)
Fragment_Kind :: enum {
	Text,
	Field,
}

@(private)
Fragment :: struct {
	kind:   Fragment_Kind,
	value:  string,
	type:   typeid,
	offset: uintptr,
}

Pattern_Error :: enum {
	None,
	Unmatched_Open_Brace,
	Unmatched_Close_Brace,
	Nested_Braces,
	Empty_Field_Name,
	Invalid_Field,
	Invalid_Type,
}

@(private)
parse_pattern :: proc(text: string, $P: typeid) -> ([]Fragment, Pattern_Error) {
	idx := 0

	next_char :: proc(idx: ^int, text: string) -> (rune, int, int, bool) {
		start := idx^
		if start >= len(text) do return {}, 0, 0, false
		ch, sz := utf8.decode_rune(text[start:])
		idx^ += sz
		return ch, start, idx^, true
	}

	fragments := make([dynamic]Fragment)
	defer builtin.delete(fragments)

	curr_start := 0
	in_field := false

	for ch, start, end in next_char(&idx, text) {
		switch ch {
		case '{':
			if in_field do return {}, .Nested_Braces
			if start > curr_start {
				append(
					&fragments,
					Fragment{kind = .Text, value = text[curr_start:start], type = nil},
				)
			}

			in_field = true
			curr_start = end
		case '}':
			if !in_field do return {}, .Unmatched_Close_Brace

			field_name := text[curr_start:start]
			if len(field_name) == 0 do return {}, .Empty_Field_Name

			field_type: typeid = nil
			offset: uintptr

			for field in reflect.struct_fields_zipped(P) {
				if field.name == field_name {
					offset = field.offset
					field_type = field.type.id
					break
				}
			}

			if field_type == nil do return {}, .Invalid_Field
			if _, found := slice.linear_search(ALLOWED_TYPES, field_type); !found {
				return {}, .Invalid_Type
			}

			append(
				&fragments,
				Fragment{kind = .Field, value = field_name, type = field_type, offset = offset},
			)

			in_field = false
			curr_start = end
		}
	}

	if in_field do return {}, .Unmatched_Open_Brace
	if curr_start < len(text) do append(&fragments, Fragment{kind = .Text, value = text[curr_start:]})

	slice := make([]Fragment, len(fragments))
	copy_slice(slice, fragments[:])

	return slice, .None
}

@(private = "file")
assign_field :: proc(ptr: rawptr, type: typeid, raw_val: string) -> bool {
	if type == string {
		str_ptr := (^string)(ptr)
		str_ptr^ = raw_val
		return true
	}

	if type == int {
		val := strconv.parse_int(raw_val) or_return
		int_ptr := (^int)(ptr)
		int_ptr^ = val
		return true
	}

	return false
}

@(private)
match_pattern :: proc(path: string, frags: []Fragment, ranges: ^[dynamic]Param_Range) -> bool {
	remaining := path
	curr_idx := 0

	for i := 0; i < len(frags); i += 1 {
		frag := frags[i]

		switch frag.kind {
		case .Text:
			if !strings.has_prefix(remaining, frag.value) do return false
			sz := len(frag.value)
			remaining = remaining[sz:]
			curr_idx += sz

		case .Field:
			extracted_len := 0

			if i + 1 < len(frags) && frags[i + 1].kind == .Text {
				next_text := frags[i + 1].value
				idx := strings.index(remaining, next_text)
				if idx < 0 do return false

				extracted_len = idx
			} else {
				extracted_len = len(remaining)
			}

			if extracted_len == 0 do return false

			append(ranges, Param_Range{curr_idx, curr_idx + extracted_len})
			remaining = remaining[extracted_len:]
			curr_idx += extracted_len
		}
	}

	return len(remaining) == 0
}

@(private)
deserialize_params :: proc(
	path: string,
	frags: []Fragment,
	ranges: []Param_Range,
	params_ptr: rawptr,
) -> (
	ok: bool,
) {
	field_idx := 0
	for frag in frags {
		if frag.kind == .Field {
			r := ranges[field_idx]
			raw_val := path[r.start:r.end]
			field_ptr := rawptr(uintptr(params_ptr) + frag.offset)
			assign_field(field_ptr, frag.type, raw_val) or_return
			field_idx += 1
		}
	}
	return true
}

@(test)
test_match_pattern_simple :: proc(t: ^testing.T) {
	Params :: struct {
		name: string,
	}

	frags, err := parse_pattern("/hello-{name}", Params)
	testing.expect_value(t, err, Pattern_Error.None)
	defer builtin.delete(frags)

	ranges := make([dynamic]Param_Range)
	defer builtin.delete(ranges)

	ok := match_pattern("/hello-odin", frags, &ranges)
	testing.expect(t, ok)
	testing.expect_value(t, len(ranges), 1)

	testing.expect_value(t, ranges[0].start, 7)
	testing.expect_value(t, ranges[0].end, 11)

	ok = match_pattern("/world-odin", frags, &ranges)
	testing.expect(t, !ok)

	ok = match_pattern("/hello-", frags, &ranges)
	testing.expect(t, !ok)
}

@(test)
test_match_pattern_multiple_fields :: proc(t: ^testing.T) {
	Params :: struct {
		user_id: int,
		post_id: int,
	}

	frags, err := parse_pattern("/users/{user_id}/posts/{post_id}", Params)
	testing.expect_value(t, err, Pattern_Error.None)
	defer builtin.delete(frags)

	ranges := make([dynamic]Param_Range)
	defer builtin.delete(ranges)
	path := "/users/42/posts/101"

	ok := match_pattern(path, frags, &ranges)
	testing.expect(t, ok)
	testing.expect_value(t, len(ranges), 2)

	testing.expect_value(t, path[ranges[0].start:ranges[0].end], "42")
	testing.expect_value(t, path[ranges[1].start:ranges[1].end], "101")
}

@(test)
test_deserialize_params_success :: proc(t: ^testing.T) {
	Params :: struct {
		id:   int,
		slug: string,
	}

	frags, err := parse_pattern("/items/{id}/{slug}", Params)
	testing.expect_value(t, err, Pattern_Error.None)
	defer builtin.delete(frags)

	path := "/items/1500/odin-lang"
	ranges := make([dynamic]Param_Range)
	defer builtin.delete(ranges)

	ok := match_pattern(path, frags, &ranges)
	testing.expect(t, ok)

	params: Params
	ok = deserialize_params(path, frags, ranges[:], &params)
	testing.expect(t, ok)

	testing.expect_value(t, params.id, 1500)
	testing.expect_value(t, params.slug, "odin-lang")
}

@(test)
test_deserialize_params_conversion_failure :: proc(t: ^testing.T) {
	Params :: struct {
		age: int,
	}

	frags, err := parse_pattern("/user/{age}", Params)
	testing.expect_value(t, err, Pattern_Error.None)
	defer builtin.delete(frags)

	path := "/user/not_a_number"
	ranges := make([dynamic]Param_Range)
	defer builtin.delete(ranges)

	ok := match_pattern(path, frags, &ranges)
	testing.expect(t, ok)

	params: Params
	ok = deserialize_params(path, frags, ranges[:], &params)
	testing.expect(t, !ok)
}

@(test)
test_parse_pattern :: proc(t: ^testing.T) {
	Params :: struct {
		name: string,
	}

	frags, err := parse_pattern("/hello-{name}", Params)
	testing.expect_value(t, err, Pattern_Error.None)
	defer builtin.delete(frags)

	testing.expect_value(t, len(frags), 2)
	testing.expect_value(t, frags[0].kind, Fragment_Kind.Text)
	testing.expect_value(t, frags[0].value, "/hello-")
	testing.expect_value(t, frags[0].type, nil)
	testing.expect_value(t, frags[1].kind, Fragment_Kind.Field)
	testing.expect_value(t, frags[1].value, "name")
	testing.expect_value(t, frags[1].type, typeid_of(string))
}

