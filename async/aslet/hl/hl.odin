package async_hl

import "core:bytes"
import "core:c"
import "core:fmt"
import "core:mem"
import "core:os"
import "core:reflect"
import "core:slice"
import "core:strings"
import "core:testing"

import "../sqlite"

Result :: sqlite.Result_Code

Open_Flag :: sqlite.Open_Flag

Conn :: struct {
	db:          ^sqlite.Connection,
	stmt_cache:  map[string]^sqlite.Statement,
	query_cache: map[string]map[typeid][dynamic]Field_Info,
}

Field_Type :: enum {
	I32,
	I64,
	F64,
	Blob,
	Bool,
	String,
	Value,
}

Field_Info :: struct {
	name:   string,
	type:   Field_Type,
	offset: uintptr,
}

Param :: union {
	i32,
	i64,
	f64,
	[]u8,
	bool,
	string,
}

Value :: union {
	i64,
	f64,
	string,
	[dynamic]u8,
}

Transaction_Mode :: enum {
	Deferred,
	Immediate,
	Exclusive,
}

open :: proc(path: string, flags: sqlite.Open_Flag = .Create | .Read_Write | .No_Mutex) -> ^Conn {
	path_cstr := strings.clone_to_cstring(path)
	defer delete(path_cstr)

	self := new(Conn)
	rc := sqlite.open_v2(path_cstr, &self.db, flags, nil)
	if rc != .Ok {
		free(self)
		return nil
	}

	self.stmt_cache = make(map[string]^sqlite.Statement)
	self.query_cache = make(map[string]map[typeid][dynamic]Field_Info)

	return self
}

close :: proc(self: ^Conn) {
	for _, stmt in self.stmt_cache {
		sqlite.finalize(stmt)
	}
	delete(self.stmt_cache)

	for sql, cache in self.query_cache {
		for _, array in cache {
			delete(array)
		}
		delete(cache)
	}
	delete(self.query_cache)

	sqlite.close(self.db)
	free(self)
}

begin :: proc(self: ^Conn, mode: Transaction_Mode = .Deferred) -> sqlite.Result_Code {
	mode_str: cstring
	switch mode {
	case .Deferred:
		mode_str = "begin deferred;"
	case .Immediate:
		mode_str = "begin immediate;"
	case .Exclusive:
		mode_str = "begin exclusive;"
	}
	return sqlite.exec(self.db, mode_str, nil, nil, nil)
}

rollback :: proc(self: ^Conn) -> sqlite.Result_Code {
	return sqlite.exec(self.db, "rollback;", nil, nil, nil)
}

commit :: proc(self: ^Conn) -> sqlite.Result_Code {
	return sqlite.exec(self.db, "commit;", nil, nil, nil)
}

batch_insert :: proc(self: ^Conn, sql: string, params: [][]Param) -> sqlite.Result_Code {
	stmt := get_stmt(self, sql) or_return
	begin(self) or_return

	for p in params {
		bind(&stmt, p)
		defer {
			sqlite.reset(stmt)
			sqlite.clear_bindings(stmt)
		}

		if res := sqlite.step(stmt); res != .Done {
			rollback(self)
			return res
		}
	}

	if res := commit(self); res != .Ok {
		rollback(self)
		return res
	}

	return .Ok
}

exec :: proc(self: ^Conn, sql: string, params: []Param = nil) -> sqlite.Result_Code {
	stmt := get_stmt(self, sql) or_return
	bind(&stmt, params)
	defer {
		sqlite.reset(stmt)
		sqlite.clear_bindings(stmt)
	}
	if res := sqlite.step(stmt); res != .Done do return res
	return .Ok
}

fetch :: proc(
	self: ^Conn,
	sql: string,
	params: []Param = nil,
	out: ^[dynamic]$T,
	limit := 0,
) -> sqlite.Result_Code {
	stmt := get_stmt(self, sql) or_return
	bind(&stmt, params)
	defer {
		sqlite.reset(stmt)
		sqlite.clear_bindings(stmt)
	}

	fields_info := get_fields_info_from_cache(self, sql, T)
	count := 0

	for sqlite.step(stmt) == .Row {
		value: T
		cols := sqlite.column_count(stmt)

		for i in 0 ..< cols {
			write_field_from_stmt(&value, &fields_info[i], stmt, i, out.allocator)
		}

		append(out, value)

		count += 1
		if count == limit do break
	}

	return .Ok
}

fetch_single :: proc(
	self: ^Conn,
	sql: string,
	params: []Param = nil,
	out: ^$T,
	allocator := context.allocator,
) -> sqlite.Result_Code {
	stmt := get_stmt(self, sql) or_return
	bind(&stmt, params)
	defer {
		sqlite.reset(stmt)
		sqlite.clear_bindings(stmt)
	}

	fields_info := get_fields_info_from_cache(self, sql, T)
	count := 0

	res := sqlite.step(stmt)
	if res != .Row do return res
	cols := sqlite.column_count(stmt)
	for i in 0 ..< cols {
		write_field_from_stmt(out, &fields_info[i], stmt, i, allocator)
	}

	return .Ok
}

get_last_error :: proc(self: ^Conn, allocator := context.allocator) -> string {
	err_cstr := sqlite.errmsg(self.db)
	return strings.clone_from_cstring(err_cstr, allocator)
}

@(private)
get_stmt :: proc(self: ^Conn, sql: string) -> (stmt: ^sqlite.Statement, rc: sqlite.Result_Code) {
	if cached, ok := self.stmt_cache[sql]; ok {
		stmt = cached
	} else {
		sql_cstr := strings.clone_to_cstring(sql)
		defer delete(sql_cstr)
		sqlite.prepare_v2(self.db, sql_cstr, c.int(len(sql)), &stmt, nil) or_return
		self.stmt_cache[sql] = stmt
	}
	return stmt, .Ok
}

@(private)
get_fields_info_from_cache :: proc(self: ^Conn, sql: string, $T: typeid) -> []Field_Info {
	sql_cache, sql_exists := self.query_cache[sql]
	if !sql_exists do sql_cache = make(map[typeid][dynamic]Field_Info)
	if type_cache, type_ok := sql_cache[T]; type_ok do return type_cache[:]

	stmt, _ := get_stmt(self, sql)
	fields := get_fields_info(T)
	defer delete(fields)

	cols := sqlite.column_count(stmt)
	ordered_fields := make([dynamic]Field_Info, cols)

	for i in 0 ..< cols {
		col_name := sqlite.column_name(stmt, i)
		field_idx := field_index(fields[:], col_name)
		assert(field_idx != -1, "unknown field")
		ordered_fields[i] = fields[field_idx]
	}

	sql_cache[T] = ordered_fields
	self.query_cache[sql] = sql_cache

	return ordered_fields[:]
}

@(private)
bind_param_at :: #force_inline proc(
	stmt: ^^sqlite.Statement,
	value: Param,
	idx: int,
) -> sqlite.Result_Code {
	c_idx := c.int(idx)
	switch v in value {
	case nil:
		sqlite.bind_null(stmt^, c_idx) or_return
	case i32:
		sqlite.bind_int(stmt^, c_idx, c.int(v)) or_return
	case i64:
		sqlite.bind_int64(stmt^, c_idx, c.int64_t(v)) or_return
	case f64:
		sqlite.bind_double(stmt^, c_idx, v) or_return
	case []u8:
		sqlite.bind_blob64(
			stmt^,
			c_idx,
			slice.as_ptr(v),
			c.int64_t(len(v)),
			{behaviour = .Transient},
		) or_return
	case bool:
		sqlite.bind_int(stmt^, c_idx, c.int(v ? 1 : 0)) or_return
	case string:
		text_cstr := strings.unsafe_string_to_cstring(v)
		sqlite.bind_text(
			stmt^,
			c_idx,
			text_cstr,
			c.int(len(v)),
			{behaviour = .Transient},
		) or_return
	}
	return .Ok
}


@(private)
bind :: proc(stmt: ^^sqlite.Statement, values: []Param) -> sqlite.Result_Code {
	for &value, i in values {
		bind_param_at(stmt, value, i + 1) or_return
	}
	return .Ok
}

@(private)
get_fields_info :: proc($T: typeid) -> [dynamic]Field_Info {
	fields_count := reflect.struct_field_count(T)
	fields := make([dynamic]Field_Info, fields_count)
	for i in 0 ..< fields_count {
		field := reflect.struct_field_at(T, i)
		name := reflect.struct_tag_lookup(field.tag, "sqlite") or_else field.name

		type, ok := field_type_from_type_id(field.type.id)
		assert(ok, "invalid field, expecting one of: i32, i64, f64, [dynamic]u8, bool, string")

		fields[i] = {
			name   = name,
			type   = type,
			offset = field.offset,
		}
	}
	return fields
}

@(private)
field_type_from_type_id :: proc(id: typeid) -> (Field_Type, bool) {
	switch id {
	case typeid_of(i32):
		return .I32, true
	case typeid_of(i64):
		return .I64, true
	case typeid_of(f64):
		return .F64, true
	case typeid_of([dynamic]u8):
		return .Blob, true
	case typeid_of(bool):
		return .Bool, true
	case typeid_of(string):
		return .String, true
	case typeid_of(Value):
		return .Value, true
	}
	return .I32, false
}

@(private)
write_field :: #force_inline proc(v: ^$T, field: ^Field_Info, value: $E) {
	base := uintptr(rawptr(v))
	field_ptr := (^E)(rawptr(base + field.offset))
	field_ptr^ = value
}

@(private)
field_index :: proc(fields: []Field_Info, name: cstring) -> int {
	for f, i in fields do if cstring_equals_string(name, f.name) do return i
	return -1
}

@(private)
cstring_equals_string :: proc(c: cstring, s: string) -> bool {
	p := ([^]u8)(c)

	for i in 0 ..< len(s) {
		if p[i] == 0 || p[i] != s[i] {
			return false
		}
	}

	return p[len(s)] == 0
}

@(private)
write_field_from_stmt :: proc(
	v: ^$T,
	field: ^Field_Info,
	stmt: ^sqlite.Statement,
	col_idx: c.int,
	allocator: mem.Allocator,
) {
	switch field.type {
	case .I32:
		value := i32(sqlite.column_int(stmt, col_idx))
		write_field(v, field, value)
	case .I64:
		value := i64(sqlite.column_int64(stmt, col_idx))
		write_field(v, field, value)
	case .F64:
		value := f64(sqlite.column_double(stmt, col_idx))
		write_field(v, field, value)
	case .Blob:
		n := sqlite.column_bytes(stmt, col_idx)
		slice := ([^]u8)(sqlite.column_blob(stmt, col_idx))[:n]
		value := make([dynamic]u8, 0, n, allocator = allocator)
		append(&value, ..slice)
		write_field(v, field, value)
	case .Bool:
		value := sqlite.column_int(stmt, col_idx) == 1
		write_field(v, field, value)
	case .String:
		value := strings.clone_from_cstring(
			sqlite.column_text(stmt, col_idx),
			allocator = allocator,
		)
		write_field(v, field, value)
	case .Value:
		ty := sqlite.column_type(stmt, col_idx)
		switch ty {
		case .Int:
			value := i64(sqlite.column_int64(stmt, col_idx))
			write_field(v, field, Value(value))
		case .Float:
			value := f64(sqlite.column_double(stmt, col_idx))
			write_field(v, field, Value(value))
		case .String:
			value := strings.clone_from_cstring(
				sqlite.column_text(stmt, col_idx),
				allocator = allocator,
			)
			write_field(v, field, Value(value))
		case .Blob:
			n := sqlite.column_bytes(stmt, col_idx)
			slice := ([^]u8)(sqlite.column_blob(stmt, col_idx))[:n]
			value := make([dynamic]u8, 0, n, allocator = allocator)
			append(&value, ..slice)
			write_field(v, field, Value(value))
		case .Null:
			write_field(v, field, Value(nil))
		}
	}
}

@(test)
test_aslet :: proc(t: ^testing.T) {
	conn := open("test.db")
	defer {
		close(conn)
		os.remove("test.db")
	}

	exec(conn, "create table person (name text, age integer, thing blob);")

	INSERT_QUERY :: "insert into person (name, age, thing) values (?1, ?2, ?3)"
	blob := [3]u8{10, 20, 30}
	exec(conn, INSERT_QUERY, {"Soreto", i32(29), blob[:]})

	FETCH_QUERY :: "select * from person"
	Person :: struct {
		name:  string,
		age:   i32,
		thing: [dynamic]u8,
	}
	out := make([dynamic]Person)
	fetch(conn, FETCH_QUERY, {}, &out)

	testing.expect(t, len(out) == 1)

	p := out[0]
	testing.expect(t, p.name == "Soreto")
	testing.expect(t, p.age == i32(29))
	testing.expect(t, bytes.equal(p.thing[:], blob[:]))

	delete(p.name)
	delete(p.thing)
	delete(out)
}

@(test)
test_batch_insert :: proc(t: ^testing.T) {
	conn := open("test2.db")
	defer {
		close(conn)
		os.remove("test2.db")
	}

	exec(conn, "create table person (name text, age integer, thing blob);")

	INSERT_QUERY :: "insert into person (name, age, thing) values (?, ?, ?)"
	blob := [3]u8{10, 20, 30}
	params := [][]Param {
		{"Soreto", i32(29), blob[:]},
		{"Soreto", i32(29), blob[:]},
		{"Soreto", i32(29), blob[:]},
		{"Soreto", i32(29), blob[:]},
		{"Soreto", i32(29), blob[:]},
	}
	batch_insert(conn, INSERT_QUERY, params)

	FETCH_QUERY :: "select count(*) from person"
	Count :: struct {
		value: i64 `sqlite:"count(*)"`,
	}
	out := make([dynamic]Count, 0, 1)
	fetch(conn, FETCH_QUERY, {}, &out)

	testing.expect(t, len(out) == 1)
	testing.expect(t, out[0].value == 5)

	delete(out)
}

@(test)
test_value :: proc(t: ^testing.T) {
	conn := open("test3.db")
	defer {
		close(conn)
		os.remove("test3.db")
	}

	exec(conn, "create table person (name text, age integer, thing blob);")

	INSERT_QUERY :: "insert into person (name, age, thing) values (?1, ?2, ?3)"
	blob := [3]u8{10, 20, 30}
	exec(conn, INSERT_QUERY, {"Soreto", nil, blob[:]})

	FETCH_QUERY :: "select * from person"
	Person :: struct {
		name:  Value,
		age:   Value,
		thing: Value,
	}
	out := make([dynamic]Person)
	fetch(conn, FETCH_QUERY, {}, &out)

	testing.expect(t, len(out) == 1)

	p := out[0]
	testing.expect(t, p.name.(string) == "Soreto")
	testing.expect(t, p.age == nil)
	testing.expect(t, bytes.equal(p.thing.([dynamic]u8)[:], blob[:]))

	if str, ok := p.name.(string); ok {
		delete(str)
	}
	if blob, ok := p.thing.([dynamic]u8); ok {
		delete(blob)
	}
	delete(out)
}

@(test)
test_fetch_single :: proc(t: ^testing.T) {
	conn := open(":memory:")
	defer close(conn)

	exec(conn, "create table person (name text, age integer, thing blob);")

	INSERT_QUERY :: "insert into person (name, age, thing) values (?1, ?2, ?3)"
	blob := [3]u8{10, 20, 30}
	exec(conn, INSERT_QUERY, {"Soreto", i32(29), blob[:]})

	FETCH_QUERY :: "select * from person"
	Person :: struct {
		name:  string,
		age:   i32,
		thing: [dynamic]u8,
	}
	p: Person
	fetch_single(conn, FETCH_QUERY, {}, &p)

	testing.expect(t, p.name == "Soreto")
	testing.expect(t, p.age == i32(29))
	testing.expect(t, bytes.equal(p.thing[:], blob[:]))

	delete(p.name)
	delete(p.thing)
}

