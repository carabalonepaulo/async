#+private
#+build !darwin
#+build !freebsd
#+build !openbsd
#+build !netbsd
#+build !linux
#+build !windows
package vmem

_reserve :: proc "contextless" (size: int) -> ([]u8, bool) {
	return nil, false
}

_release :: proc "contextless" (block: []u8) {}

