# async

Single threaded cooperative coroutine scheduler, all primitives included.

### Why single threaded?

Stateful applications can share state with no sync overhead. You don't have to make everything thread safe just because a few tasks are running concurrently.

This doesn't mean you can't use multiple threads. The http server is a good example: it's multi threaded, with each worker running its own independent scheduler.

### Primitives
- spawn
- yield/wake
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
- broadcaster

### Packages
- io (nbio)
- http client (curl)
- http server
- aslet (offload sqlite)

### Usage

The runtime can be integrated with `async.poll`/`async.block`/`async.run`. Must be initialized per thread with `async.init`/`async.deinit`.

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

	async.run()
}
```

### Contributions

Suggestions and bug reports are welcome. I'll fix actual bugs, but I'm not interested in debating technical decisions or reviewing external code, whether written by a person or ai.
