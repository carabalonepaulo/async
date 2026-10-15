package async_io

import ".."
import "core:nbio"
import "core:net"

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
	cancel: Maybe(async.Cancel_Token) = nil,
) -> (
	client: net.TCP_Socket,
	ep: net.Endpoint,
	err: net.Accept_Error,
) {
	cb := proc(op: ^nbio.Operation) {
		os := get_one_shot(op, Accept_Result)
		async.send(os, Accept_Result{op.accept.client, op.accept.client_endpoint, op.accept.err})
	}
	op := nbio.accept(socket, cb, nbio.NO_TIMEOUT)
	res := try(op, cancel, Accept_Result, net.Accept_Error.Timeout) or_return
	return res.client, res.client_endpoint, res.err
}

@(private)
Dial_Result :: struct {
	sock: net.TCP_Socket,
	err:  net.Network_Error,
}

dial :: proc(
	endpoint: net.Endpoint,
	cancel: Maybe(async.Cancel_Token) = nil,
) -> (
	sock: net.TCP_Socket,
	err: net.Network_Error,
) {
	cb := proc(op: ^nbio.Operation) {
		os := get_one_shot(op, Dial_Result)
		async.send(os, Dial_Result{op.dial.socket, op.dial.err})
	}
	op := nbio.dial(endpoint, cb, nbio.NO_TIMEOUT)
	res := try(op, cancel, Dial_Result, net.Dial_Error.Timeout) or_return
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
	cancel: Maybe(async.Cancel_Token) = nil,
) -> (
	n: int,
	err: nbio.Recv_Error,
) {
	cb := proc(op: ^nbio.Operation) {
		os := get_one_shot(op, Recv_Result)
		async.send(os, Recv_Result{op.recv.received, op.recv.err})
	}
	op := nbio.recv(socket, bufs, cb, all, nbio.NO_TIMEOUT)

	cancel_err := get_socket_cancel_error(
		socket,
		nbio.Recv_Error,
		net.TCP_Recv_Error.Timeout,
		net.UDP_Recv_Error.Timeout,
	)
	res := try(op, cancel, Recv_Result, cancel_err) or_return
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
	cancel: Maybe(async.Cancel_Token) = nil,
) -> (
	sent: int,
	err: nbio.Send_Error,
) {
	cb := proc(op: ^nbio.Operation) {
		os := get_one_shot(op, Send_Result)
		async.send(os, Send_Result{op.send.sent, op.send.err})
	}
	op := nbio.send(socket, bufs, cb, endpoint, all, nbio.NO_TIMEOUT)

	cancel_err := get_socket_cancel_error(
		socket,
		nbio.Send_Error,
		net.TCP_Send_Error.Timeout,
		net.UDP_Send_Error.Timeout,
	)
	res := try(op, cancel, Send_Result, cancel_err) or_return
	return res.sent, res.err
}

send_file :: proc(
	socket: net.TCP_Socket,
	file: Handle,
	offset: int = 0,
	nbytes: int = SEND_ENTIRE_FILE,
	cancel: Maybe(async.Cancel_Token) = nil,
) -> nbio.Send_File_Error {
	cb := proc(op: ^nbio.Operation) {
		os := get_one_shot(op, nbio.Send_File_Error)
		async.send(os, op.sendfile.err)
	}
	op := nbio.sendfile(socket, file, cb, offset, nbytes, false, nbio.NO_TIMEOUT)
	return try(op, cancel, nbio.Send_File_Error, nbio.FS_Error.Timeout) or_return
}

