package async_io

import ".."
import "core:nbio"
import "core:net"

@(private)
get_one_shot :: #force_inline proc(op: ^nbio.Operation, $T: typeid) -> async.One_Shot(T) {
	return transmute(async.One_Shot(T))(op.user_data[0])
}

@(private)
try :: proc(
	op: ^nbio.Operation,
	cancel: Maybe(async.Cancel_Token),
	$T: typeid,
	err: $E,
) -> (
	T,
	E,
) {
	os := async.create_one_shot(T)
	op.user_data[0] = transmute(rawptr)(os)

	if cancel, cancel_ok := cancel.(async.Cancel_Token); cancel_ok {
		res: T
		idx := async.select({async.branch(cancel), async.branch(os, &res)})
		if idx == 0 {
			nbio.remove(op)
			async.destroy(os)
			return {}, err
		} else do return res, {}
	} else {
		res := async.recv(os)
		return res, {}
	}
}

@(private)
get_socket_cancel_error :: proc(sock: net.Any_Socket, $U: typeid, tcp_err: $A, udp_err: $B) -> U {
	if _, ok := sock.(net.TCP_Socket); ok {
		return tcp_err
	} else {
		return udp_err
	}
}

