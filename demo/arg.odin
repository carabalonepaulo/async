package main

import "core:fmt"

import "../async"

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
	async.block(async.spawn(person, arg_coro))
}
