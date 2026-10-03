package async_http_server_router_v2

import "base:builtin"
import "core:reflect"
import "core:slice"
import "core:strconv"
import "core:strings"
import "core:testing"

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
	Empty_Field_Name,
	Invalid_Field,
	Invalid_Type,
	Invalid_Segment_Format,
}

@(private = "file")
find_field :: proc(
	$T: typeid,
	field_name: string,
) -> (
	offset: uintptr,
	field_type: typeid,
	err: Pattern_Error,
) {
	if len(field_name) == 0 do return {}, {}, .Empty_Field_Name

	for field in reflect.struct_fields_zipped(T) {
		if field.name == field_name {
			offset = field.offset
			field_type = field.type.id
			break
		}
	}

	if field_type == nil do return {}, {}, .Invalid_Field
	if _, found := slice.linear_search(ALLOWED_TYPES, field_type); !found {
		return {}, {}, .Invalid_Type
	}

	return offset, field_type, .None
}

@(private)
parse_pattern :: proc(text: string, $P: typeid) -> (fragments: []Fragment, err: Pattern_Error) {
	frags := make([dynamic]Fragment)
	defer builtin.delete(frags)

	rest := text
	for seg in strings.split_iterator(&rest, "/") {
		if len(seg) == 0 do continue

		if len(seg) >= 2 && seg[0] == '{' && seg[len(seg) - 1] == '}' {
			field_name := seg[1:len(seg) - 1]
			offset, field_type := find_field(P, field_name) or_return

			append(
				&frags,
				Fragment{kind = .Field, value = field_name, offset = offset, type = field_type},
			)
		} else {
			if strings.contains_any(seg, "{}") {
				return {}, .Invalid_Segment_Format
			}
			append(&frags, Fragment{kind = .Text, value = seg})
		}
	}

	return slice.clone(frags[:]), .None
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
	clear(ranges)

	rest := path
	frag_idx := 0
	curr_byte_idx := 0

	for seg in strings.split_iterator(&rest, "/") {
		for curr_byte_idx < len(path) && path[curr_byte_idx] == '/' {
			curr_byte_idx += 1
		}

		if len(seg) == 0 do continue

		if frag_idx >= len(frags) do return false

		frag := frags[frag_idx]
		frag_idx += 1

		switch frag.kind {
		case .Text:
			if seg != frag.value do return false
		case .Field:
			seg_start := curr_byte_idx
			seg_end := curr_byte_idx + len(seg)
			append(ranges, Param_Range{start = seg_start, end = seg_end})
		}

		curr_byte_idx += len(seg)
	}

	return frag_idx == len(frags)
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
test_parse_pattern :: proc(t: ^testing.T) {
	Params :: struct {
		name: string,
	}

	frags, err := parse_pattern("/hello/{name}", Params)
	testing.expect_value(t, err, Pattern_Error.None)
	defer builtin.delete(frags)

	testing.expect_value(t, len(frags), 2)
	testing.expect_value(t, frags[0].kind, Fragment_Kind.Text)
	testing.expect_value(t, frags[0].value, "hello")
	testing.expect_value(t, frags[1].kind, Fragment_Kind.Field)
	testing.expect_value(t, frags[1].value, "name")

	_, err_invalid := parse_pattern("/hello-{name}", Params)
	testing.expect_value(t, err_invalid, Pattern_Error.Invalid_Segment_Format)
}

@(test)
test_match_pattern_simple :: proc(t: ^testing.T) {
	Params :: struct {
		name: string,
	}

	frags, err := parse_pattern("/hello/{name}", Params)
	testing.expect_value(t, err, Pattern_Error.None)
	defer builtin.delete(frags)

	ranges := make([dynamic]Param_Range)
	defer builtin.delete(ranges)

	ok := match_pattern("/hello/odin", frags, &ranges)
	testing.expect(t, ok)
	testing.expect_value(t, len(ranges), 1)

	testing.expect_value(t, ranges[0].start, 7)
	testing.expect_value(t, ranges[0].end, 11)

	ok = match_pattern("/world/odin", frags, &ranges)
	testing.expect(t, !ok)

	ok = match_pattern("/hello/", frags, &ranges)
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

