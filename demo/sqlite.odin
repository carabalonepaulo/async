package main

import "core:fmt"
import "core:sync/chan"

import "../async"
import "../async/aslet"

CREATE :: `create table person (
	    id integer primary key autoincrement,
	    name text not null unique,
	    age integer not null
	);`

INSERT :: `insert into person (name, age) values (?1, ?2);`

SELECT :: `select * from person;`

SELECT_SCALAR :: `select age from person`

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

	context.allocator = context.temp_allocator

	out := make([dynamic]Person)
	assert(aslet.fetch(&conn, SELECT, {}, &out) == .Ok)
	fmt.println("[sqlite] fetch", out)

	person: Person
	assert(aslet.fetch_single(&conn, SELECT, {}, &person) == .Ok)
	fmt.println("[sqlite] fetch single", person)

	age: i64
	assert(aslet.fetch_scalar(&conn, SELECT_SCALAR, {}, &age) == .Ok)
	fmt.println("[sqlite] fetch scalar", age)

	tx, ok := aslet.transaction(&conn)
	assert(ok)
	assert(aslet.fetch_single(&tx, SELECT, {}, &person) == .Ok)
	assert(aslet.fetch_scalar(&tx, SELECT_SCALAR, {}, &age) == .Ok)
	aslet.rollback(&tx)
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
