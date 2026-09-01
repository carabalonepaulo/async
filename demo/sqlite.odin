package main

import "../async"
import "../async/aslet"
import "core:fmt"

CREATE :: `create table person (
	    id integer primary key autoincrement,
	    name text not null unique,
	    age integer not null
	);`

INSERT :: `insert into person (name, age) values (?1, ?2);`

SELECT :: `select * from person;`

sqlite_task :: proc(a: ^aslet.Aslet) {
	conn, conn_ok := aslet.open(a, "test.db")
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
	a: aslet.Aslet
	assert(aslet.init(&a, 1024) == nil, "failed to init aslet")
	defer aslet.deinit(&a)

	async.spawn(&a, sqlite_task)

	for async.get_pending() > 0 {
		async.poll()
		aslet.poll(&a)
	}
}

