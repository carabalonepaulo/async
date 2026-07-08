package async_io

import ".."
import "core:fmt"
import "core:nbio"
import "core:net"
import "core:time"

SEND_ENTIRE_FILE :: nbio.SEND_ENTIRE_FILE

create_socket :: #force_inline proc(
	family: net.Address_Family,
	protocol: net.Socket_Protocol,
	loc := #caller_location,
) -> (
	net.Any_Socket,
	net.Create_Socket_Error,
) {
	return nbio.create_socket(family, protocol, nil, loc)
}

create_tcp_socket :: #force_inline proc(
	family: net.Address_Family,
	loc := #caller_location,
) -> (
	net.TCP_Socket,
	net.Create_Socket_Error,
) {
	return nbio.create_tcp_socket(family, nil, loc)
}

create_udp_socket :: #force_inline proc(
	family: net.Address_Family,
	loc := #caller_location,
) -> (
	net.UDP_Socket,
	net.Create_Socket_Error,
) {
	return nbio.create_udp_socket(family, nil, loc)
}

bind :: #force_inline proc(socket: net.Any_Socket, endpoint: net.Endpoint) -> net.Bind_Error {
	return nbio.bind(socket, endpoint)
}

listen_tcp :: proc(
	endpoint: net.Endpoint,
	backlog: int = 1000,
	loc := #caller_location,
) -> (
	net.TCP_Socket,
	net.Network_Error,
) {
	return nbio.listen_tcp(endpoint, backlog, nil, loc)
}

@(private)
Accept_Result :: struct {
	client:          net.TCP_Socket,
	client_endpoint: net.Endpoint,
	err:             net.Accept_Error,
}

accept :: proc(
	socket: net.TCP_Socket,
	timeout: time.Duration = NO_TIMEOUT,
) -> (
	net.TCP_Socket,
	net.Endpoint,
	net.Accept_Error,
) {
	cb := proc(op: ^nbio.Operation) {
		async.send(
			load_handle(op),
			Accept_Result{op.accept.client, op.accept.client_endpoint, op.accept.err},
		)
	}
	op := nbio.accept(socket, cb, timeout)
	store_handle(op)
	res := async.recv(Accept_Result)
	return res.client, res.client_endpoint, res.err
}

@(private)
Dial_Result :: struct {
	sock: net.TCP_Socket,
	err:  net.Network_Error,
}

dial :: proc(
	endpoint: net.Endpoint,
	timeout: time.Duration = NO_TIMEOUT,
) -> (
	net.TCP_Socket,
	net.Network_Error,
) {
	cb := proc(op: ^nbio.Operation) {
		async.send(load_handle(op), Dial_Result{op.dial.socket, op.dial.err})
	}
	op := nbio.dial(endpoint, cb, timeout)
	store_handle(op)
	res := async.recv(Dial_Result)
	return res.sock, res.err
}

@(private)
Recv_Result :: struct {
	received: int,
	err:      nbio.Recv_Error,
}

recv :: proc(
	socket: net.Any_Socket,
	bufs: [][]u8,
	all: bool = false,
	timeout: time.Duration = NO_TIMEOUT,
) -> (
	int,
	nbio.Recv_Error,
) {
	cb := proc(op: ^nbio.Operation) {
		async.send(load_handle(op), Recv_Result{op.recv.received, op.recv.err})
	}
	op := nbio.recv(socket, bufs, cb, all, timeout)
	store_handle(op)
	res := async.recv(Recv_Result)
	return res.received, res.err
}

@(private)
Send_Result :: struct {
	sent: int,
	err:  nbio.Send_Error,
}

send :: proc(
	socket: net.Any_Socket,
	bufs: [][]u8,
	endpoint: net.Endpoint = {},
	all: bool = true,
	timeout: time.Duration = NO_TIMEOUT,
) -> (
	int,
	nbio.Send_Error,
) {
	cb := proc(op: ^nbio.Operation) {
		async.send(load_handle(op), Send_Result{op.send.sent, op.send.err})
	}
	op := nbio.send(socket, bufs, cb, endpoint, all, timeout)
	store_handle(op)
	res := async.recv(Send_Result)
	return res.sent, res.err
}

send_file :: proc {
	send_file_without_progress,
	send_file_with_progress,
}

send_file_without_progress :: proc(
	socket: net.TCP_Socket,
	file: Handle,
	offset: int = 0,
	nbytes: int = SEND_ENTIRE_FILE,
	timeout: time.Duration = NO_TIMEOUT,
) -> nbio.Send_File_Error {
	cb := proc(op: ^nbio.Operation) {async.send(load_handle(op), op.sendfile.err)}
	op := nbio.sendfile(socket, file, cb, offset, nbytes, false, timeout)
	store_handle(op)
	return async.recv(nbio.Send_File_Error)
}

Send_File_Status :: struct {
	file:    Handle,
	sent:    int,
	err:     nbio.Send_File_Error,
	is_done: bool,
}

send_file_with_progress :: proc(
	socket: net.TCP_Socket,
	file: Handle,
	offset: int = 0,
	nbytes: int = SEND_ENTIRE_FILE,
	progress: async.Chan(Send_File_Status),
	timeout: time.Duration = NO_TIMEOUT,
) {
	cb := proc(op: ^nbio.Operation) {
		progress := async.chan_from_rawptr(Send_File_Status, op.user_data[0])
		status := Send_File_Status {
			file    = op.sendfile.file,
			sent    = op.sendfile.sent,
			err     = op.sendfile.err,
			is_done = op.sendfile.sent == op.sendfile.nbytes || op.sendfile.err != nil,
		}
		async.send(progress, status)
	}
	op := nbio.sendfile(socket, file, cb, offset, nbytes, true, timeout)
	op.user_data[0] = async.into_rawptr(progress)
}

