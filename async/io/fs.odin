package async_io

import ".."

import "core:nbio"
import "core:time"

CWD :: nbio.CWD
NO_TIMEOUT :: nbio.NO_TIMEOUT

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
) -> (
	Handle,
	FS_Error,
) {
	cb := proc(op: ^nbio.Operation) {
		async.send(load_handle(op), Open_Result{op.open.handle, op.open.err})
	}
	op := nbio.open(path, cb, mode, perm, dir)
	store_handle(op)
	res := async.recv(Open_Result)
	return res.handle, res.err
}

read :: proc(
	handle: Handle,
	offset: int,
	buf: []u8,
	all := false,
	timeout: time.Duration = NO_TIMEOUT,
) -> FS_Error {
	cb := proc(op: ^nbio.Operation) {async.send(load_handle(op), op.read.err)}
	op := nbio.read(handle, offset, buf, cb, all, timeout)
	store_handle(op)
	return async.recv(FS_Error)
}

@(private)
Read_Entire_File_Result :: struct {
	buf: []u8,
	err: nbio.Read_Entire_File_Error,
}

read_entire_file :: proc(
	path: string,
	allocator := context.allocator,
	dir: Handle = nbio.CWD,
	loc := #caller_location,
) -> (
	[]u8,
	nbio.Read_Entire_File_Error,
) {
	cb := proc(ud: rawptr, data: []u8, err: nbio.Read_Entire_File_Error) {
		handle := transmute(async.Handle)(ud)
		async.send(handle, Read_Entire_File_Result{data, err})
	}
	handle := transmute(rawptr)(async.get_handle())
	nbio.read_entire_file(path, handle, cb, allocator, dir, nil, loc)
	res := async.recv(Read_Entire_File_Result)
	return res.buf, res.err
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
	timeout: time.Duration = NO_TIMEOUT,
) -> (
	int,
	FS_Error,
) {
	cb := proc(op: ^nbio.Operation) {
		async.send(load_handle(op), Write_Result{op.write.written, op.write.err})
	}
	op := nbio.write(handle, offset, buf, cb, all, timeout)
	store_handle(op)
	res := async.recv(Write_Result)
	return res.written, res.err
}

@(private)
Stat_Result :: struct {
	type: File_Type,
	size: i64,
	err:  FS_Error,
}

stat :: proc(handle: Handle) -> (File_Type, i64, FS_Error) {
	cb := proc(op: ^nbio.Operation) {
		async.send(load_handle(op), Stat_Result{op.stat.type, op.stat.size, op.stat.err})
	}
	op := nbio.stat(handle, cb)
	store_handle(op)
	res := async.recv(Stat_Result)
	return res.type, res.size, res.err
}

close :: proc(handle: Handle) {
	cb := proc(op: ^nbio.Operation) {async.wake(load_handle(op))}
	op := nbio.close(handle, cb)
	store_handle(op)
	async.yield()
}

