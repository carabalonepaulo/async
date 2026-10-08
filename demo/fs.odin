package main

import "core:fmt"
import "core:os"

import "../async"
import "../async/io"

FILE_NAME :: "hello.txt"

actions :: proc() {
	write_file()
	read_file()
}

write_file :: proc() {
	file, open_err := io.open(FILE_NAME, {.Create, .Write, .Append})
	assert(open_err == .None)

	text := "hello world!"
	n, write_err := io.write(file, 0, transmute([]u8)(text))
	assert(write_err == .None)
	fmt.printfln("[write_file] %v/%v", n, len(text))

	io.close(file)
}

read_file :: proc() {
	text, err := io.read_entire_file(FILE_NAME)
	assert(err.value == .None)
	defer delete(text)

	fmt.println("[read_file]", transmute(string)(text))
}

fs_demo :: proc() {
	async.block(async.spawn(actions))
	os.remove(FILE_NAME)
}
