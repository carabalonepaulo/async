package async_io

import "core:nbio"

init :: proc() {
	nbio.acquire_thread_event_loop()
}

deinit :: proc() {
	nbio.release_thread_event_loop()
}

poll :: proc() {
	nbio.tick()
}

