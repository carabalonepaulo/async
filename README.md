# async

Cooperative coroutine scheduler, all primitives included.

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
- [http client](https://github.com/carabalonepaulo/http_client)
- [http server](https://github.com/carabalonepaulo/http_server)
- [sqlite](https://github.com/carabalonepaulo/sqlite)

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
