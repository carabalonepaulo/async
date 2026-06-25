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
	sched: async.Scheduler
	async.init(&sched)
	defer async.deinit(&sched)

	person := Person{"Soreto", 30}
	async.spawn(&sched, person, arg_coro)

	for async.get_pending(&sched) > 0 {
		async.poll(&sched)
		time.sleep(1 * time.Millisecond)
	}
}

