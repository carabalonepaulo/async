package async_io

import ".."
import "core:nbio"

NO_TIMEOUT :: nbio.NO_TIMEOUT

Closable :: nbio.Closable

init :: proc() {
	nbio.acquire_thread_event_loop()
}

deinit :: proc() {
	nbio.release_thread_event_loop()
}

poll :: proc() {
	nbio.tick(0)
}

close :: proc(closable: Closable) {
	nbio.close(closable)
}

