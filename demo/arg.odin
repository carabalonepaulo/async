package main

import "../async"
import "core:fmt"
import "core:time"

Person :: struct {
	name: string,
	age:  int,
}

arg_coro :: proc(person: Person) {
	fmt.println("name:", person.name)
	fmt.println("age:", person.age)
}

arg_demo :: proc() {
	person := Person{"Soreto", 30}
	async.spawn(person, arg_coro)

	for async.get_pending() > 0 {
		async.poll()
		time.sleep(1 * time.Millisecond)
	}
}

