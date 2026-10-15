package async_io

import ".."
import "core:nbio"

NO_TIMEOUT :: nbio.NO_TIMEOUT

Closable :: nbio.Closable

init :: proc() {
	nbio.acquire_thread_event_loop()
	async.schedule(proc() -> bool {
		nbio.tick(0)
		return true
	})
}

deinit :: proc() {
	nbio.release_thread_event_loop()
}

close :: proc(closable: Closable) {
	nbio.close(closable)
}
