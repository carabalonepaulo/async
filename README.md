# async

A lightweight stackful coroutine runtime for Odin.

`async` is a cooperative, thread-local runtime built on top of minicoro. It provides stackful coroutines, channels, event selection, timers and cancellation primitives while remaining explicit, predictable and easy to integrate with external systems.

The runtime is designed for stateful applications such as game servers, simulations and event-driven systems where explicit ownership and deterministic execution are preferred over implicit multithreading.

## Features

- Stackful coroutines
- Cooperative scheduling
- Thread-local scheduler
- Channels
- `select` (wait for any)
- `all` (wait for all)
- Timers and `sleep`
- Cancellation tokens
- Broadcast signals
- Coroutine mailboxes
- No hidden threads
- Explicit resource lifetime

## Example

```odin
coroutine :: proc(client: ^http.Client) {
	out := async.create_chan(http.Result)
	defer async.destroy(out)

	cancel := async.create_cancel_token()
	async.cancel_after(cancel, 1 * time.Millisecond)

	http.fetch(client, http.Request{
		method = .Get,
		url = "https://httpbin.org/delay/5",
		out = out,
		cancel = cancel,
	})

	res: http.Result

	switch async.select({
		async.branch(out, &res),
		async.branch(cancel),
	}) {
	case 0:
		fmt.println("request completed")
	case 1:
		fmt.println("request cancelled")
	}
}

main :: proc() {
	async.init()
	defer async.deinit()

	client: http.Client
	http.init(&client)
	defer http.deinit(&client)

	async.spawn(&client, coroutine)
	async.run(&client, http.poll, 1 * time.Millisecond)
}
```

## Runtime model

A scheduler owns every coroutine, channel, timer and synchronization primitive created inside it.

Schedulers are thread-local and never migrate work between threads.

This provides:

- predictable execution
- explicit ownership
- lock-free scheduler internals
- resource validation through generational handles

Parallelism is achieved by running multiple schedulers on different threads rather than sharing a scheduler across threads.

## Building blocks

The runtime intentionally exposes only a small number of primitives.

- Coroutines
- Channels
- Timers

Higher-level abstractions are implemented on top of them.

- `select`
- `all`
- `Signal`
- `Cancellation_Token`

## Resource management

Resources created through the scheduler must be explicitly destroyed.

```odin
ch := async.create_chan(int)
defer async.destroy(ch)
```

The only exception is `Cancellation_Token`, which is automatically cleaned up during scheduler shutdown if left alive.

## Philosophy

`async` is intentionally not an async/await runtime.

It does not provide:

- futures
- promises
- work stealing
- automatic thread pools
- implicit synchronization

Instead it focuses on:

- explicit ownership
- cooperative execution
- composable primitives
- easy integration with external event loops
- deterministic behavior

## Status

The public API is approaching stability, although performance optimizations are still ongoing.
