package async_io

import ".."
import "core:nbio"
import "core:os"

CWD :: nbio.CWD

Permissions_All :: nbio.Permissions_All
Permissions_Default_Directory :: nbio.Permissions_Default_Directory
Permissions_Default_File :: nbio.Permissions_Default_File
Permissions_Execute_All :: nbio.Permissions_Execute_All
Permissions_Read_All :: nbio.Permissions_Read_All
Permissions_Read_Write_All :: nbio.Permissions_Read_Write_All
Permissions_Write_All :: nbio.Permissions_Write_All

FS_Error :: nbio.FS_Error
File_Flag :: nbio.File_Flag
Permissions :: nbio.Permissions
Handle :: nbio.Handle
File_Type :: nbio.File_Type

@(private)
Open_Result :: struct {
	handle: Handle,
	err:    FS_Error,
}

open :: proc(
	path: string,
	mode: bit_set[File_Flag;int] = {.Read},
	perm: Permissions = Permissions_Default_File,
	dir: nbio.Handle = nbio.CWD,
	cancel: Maybe(async.Cancel_Token) = nil,
) -> (
	handle: Handle,
	err: FS_Error,
) {
	cb := proc(op: ^nbio.Operation) {
		os := get_one_shot(op, Open_Result)
		async.send(os, Open_Result{op.open.handle, op.open.err})
	}
	op := nbio.open(path, cb, mode, perm, dir)
	res := try(op, cancel, Open_Result, FS_Error.Timeout) or_return
	return res.handle, res.err
}

read :: proc(
	handle: Handle,
	offset: int,
	buf: []u8,
	all := false,
	cancel: Maybe(async.Cancel_Token) = nil,
) -> FS_Error {
	cb := proc(op: ^nbio.Operation) {
		os := get_one_shot(op, FS_Error)
		async.send(os, op.read.err)
	}
	op := nbio.read(handle, offset, buf, cb, all, nbio.NO_TIMEOUT)
	return try(op, cancel, FS_Error, FS_Error.Timeout) or_return
}

Read_Entire_File_Error :: nbio.Read_Entire_File_Error

@(private)
Read_Entire_File_Result :: struct {
	buf: []u8,
	err: nbio.Read_Entire_File_Error,
}

read_entire_file :: proc(
	path: string,
	allocator := context.allocator,
	dir: Handle = nbio.CWD,
	cancel: Maybe(async.Cancel_Token) = nil,
) -> (
	buf: []u8,
	err: Read_Entire_File_Error,
) {
	file, open_err := open(path, {.Read}, cancel = cancel)
	if open_err != .None do return {}, Read_Entire_File_Error{.Open, open_err}
	defer close(file)

	type, size, stat_err := stat(file, cancel)
	if stat_err != .None do return {}, Read_Entire_File_Error{.Stat, stat_err}
	if type != .Regular do return {}, Read_Entire_File_Error{.Stat, .Unsupported}

	read_buf, alloc_err := make([]u8, size)
	if alloc_err != nil do return {}, Read_Entire_File_Error{.Read, .Allocation_Failed}
	defer if err.operation != .None do delete(read_buf)

	read_err := read(file, 0, read_buf, true, cancel)
	if read_err != nil do return {}, Read_Entire_File_Error{.Read, read_err}

	return read_buf, {}
}

Write_Entire_File_Error :: nbio.Read_Entire_File_Error

write_entire_file :: proc(
	path: string,
	buf: []u8,
	dir: Handle = nbio.CWD,
	cancel: Maybe(async.Cancel_Token) = nil,
) -> (
	err: Write_Entire_File_Error,
) {
	file, open_err := open(path, {.Create, .Write, .Trunc})
	if open_err != .None do return Write_Entire_File_Error{.Open, open_err}
	defer close(file)

	_, write_err := write(file, 0, buf, true, cancel)
	if write_err != .None do return Write_Entire_File_Error{.Write, write_err}

	return {}
}

@(private)
Write_Result :: struct {
	written: int,
	err:     FS_Error,
}

write :: proc(
	handle: Handle,
	offset: int,
	buf: []u8,
	all := true,
	cancel: Maybe(async.Cancel_Token) = nil,
) -> (
	written: int,
	err: FS_Error,
) {
	cb := proc(op: ^nbio.Operation) {
		os := get_one_shot(op, Write_Result)
		async.send(os, Write_Result{op.write.written, op.write.err})
	}
	op := nbio.write(handle, offset, buf, cb, all, nbio.NO_TIMEOUT)
	res := try(op, cancel, Write_Result, FS_Error.Timeout) or_return
	return res.written, res.err
}

@(private)
Stat_Result :: struct {
	type: File_Type,
	size: i64,
	err:  FS_Error,
}

stat :: proc(
	handle: Handle,
	cancel: Maybe(async.Cancel_Token) = nil,
) -> (
	type: File_Type,
	size: i64,
	err: FS_Error,
) {
	cb := proc(op: ^nbio.Operation) {
		os := get_one_shot(op, Stat_Result)
		async.send(os, Stat_Result{op.stat.type, op.stat.size, op.stat.err})
	}
	op := nbio.stat(handle, cb)
	res := try(op, cancel, Stat_Result, FS_Error.Timeout) or_return
	return res.type, res.size, res.err
}

Read_Dir :: distinct os.Read_Directory_Iterator

create_read_dir :: proc(path: string) -> (it: Read_Dir, ok: bool) {
	file, err := os.open(path, {.Read})
	if err != nil do return {}, false

	raw_it := os.read_directory_iterator_create(file)
	if raw_it.err.err != nil {
		os.close(file)
		return {}, false
	}

	return Read_Dir(raw_it), true
}

destroy_read_dir :: proc(it: ^Read_Dir) {
	raw := (^os.Read_Directory_Iterator)(it)
	os.close(raw.f)
	os.read_directory_iterator_destroy(raw)
}

read_dir :: proc(it: ^Read_Dir) -> (os.File_Info, int, bool) {
	async.reschedule()
	return os.read_directory_iterator((^os.Read_Directory_Iterator)(it))
}

read_dir_error :: proc(it: ^Read_Dir) -> (string, os.Error) {
	return os.read_directory_iterator_error((^os.Read_Directory_Iterator)(it))
}

