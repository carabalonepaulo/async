# async

Single threaded cooperative coroutine scheduler, all primitives included.

### Primitives
- spawn
- yield/wake
- recv/send (wake with value)
- join/join_many
- sleep/sleep_or_cancel
- timer/next_tick/reschedule
- select/all (wait for any or all branches)
- channel
- cancel token
- signal (unbuffered broadcaster)
- wait group
- semaphore
- one shot

### Packages
- io (nbio)
- http client (curl)
- http server
- aslet (offload sqlite)

### Usage
The runtime can be integrated with `async.poll`/`async.block`/`async.run`. Must be initialized per thread with `async.init`/`async.deinit`. Every module has its own `poll` like `io.poll` or `http.poll`, you pay for what you are using.

```odin
package main

import "../async"
import "../async/io"

main :: proc() {
	async.init()
	defer async.deinit()

	io.init()
	defer io.deinit()

	// async.spawn(...)

	async.run(io.poll)
}
```
