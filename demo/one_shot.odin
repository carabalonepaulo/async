package main

import "../async"
import "core:fmt"
import "core:time"

one_shot_demo :: proc() {
	a := async.spawn(proc() {
		b := async.spawn(proc() {
			os := async.create_one_shot(int)
			async.spawn(os, proc(os: async.One_Shot(int)) {
				fmt.println("[one shot b] send(123)")
				async.send(os, 123)

				fmt.printfln("[one shot b] try_send(456) == %v", async.try_send(os, 456))
			})

			fmt.println("[one shot b] before")

			value, value_ok := async.recv(os)
			fmt.println("[one shot b] after:", value, value_ok)

			value, value_ok = async.try_recv(os)
			fmt.printfln("[one shot b] try_send == %v", value_ok)
		})

		c := async.spawn(proc() {
			os := async.create_one_shot(int)
			async.spawn(os, proc(os: async.One_Shot(int)) {
				fmt.println("[one shot c] send(123)")
				async.send(os, 123)
			})

			fmt.println("[one shot c] before")

			value: int
			value_ok: bool

			idx := async.select({async.branch(os, &value, &value_ok)})
			if idx == 0 do fmt.println("[one shot c] after:", value, value_ok)
		})

		async.join_many({b, c})
	})

	async.block(a, 1 * time.Millisecond)
}

