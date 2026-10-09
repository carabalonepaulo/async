package main

import "core:fmt"

import "../async"
import "../async/aslet"

CREATE :: `create table person (
	    id integer primary key autoincrement,
	    name text not null unique,
	    age integer not null
	);`

INSERT :: `insert into person (name, age) values (?1, ?2);`

SELECT :: `select * from person;`

sqlite_task :: proc(a: ^aslet.Consumer) {
	cancel := async.create_cancel_token()
	// async.trigger(cancel)

	conn, conn_ok := aslet.open(a, "test.db", cancel = cancel)
	assert(conn_ok)
	defer aslet.close(&conn)

	fmt.println("[sqlite] create", aslet.exec(&conn, CREATE, {}))
	fmt.println("[sqlite] insert", aslet.exec(&conn, INSERT, {"soreto", i32(29)}))

	Person :: struct {
		id:   i32,
		name: string,
		age:  i32,
	}

	out := make([dynamic]Person, allocator = context.temp_allocator)
	assert(aslet.fetch(&conn, SELECT, {}, &out) == .Ok)
	fmt.println("[sqlite]", out)
}

sqlite_demo :: proc() {
	w: aslet.Worker
	aslet.init(&w)
	defer aslet.deinit(&w)

	a, _ := aslet.create_consumer(&w)
	defer aslet.destroy(a)

	async.spawn(a, sqlite_task)
	async.run()
}
