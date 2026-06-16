# async

Extensible coroutine scheduler for Odin built on top of minicoro.

## Overview

`async` is a single-threaded cooperative scheduler for Odin based on stackful coroutines provided by minicoro.

The library is designed around a small core scheduler that can be extended with additional asynchronous primitives such as timers, networking, databases, and filesystem operations.

## Features

- Single-threaded cooperative scheduling
- Stackful coroutines via minicoro
- Task spawning
- Coroutine suspension and resumption
- Built-in sleep support
- Extensible wake/scheduler architecture
- No global scheduler state
- Multiple scheduler instances supported

## Example

```odin
package main

import async "async:scheduler"
import "core:fmt"
import "core:time"

producer :: proc(handle: async.Handle) {
	for i in 1 ..= 5 {
		fmt.println("[producer] sent", i)
		async.send(handle, i)
		async.reschedule()
	}
}

consumer :: proc() {
	for _ in 0 ..< 5 {
		value := async.recv(int)
		fmt.println("[consumer]", value)
	}
}

main :: proc() {
	sched: async.Scheduler
	async.init(&sched)
	defer async.deinit(&sched)

	consumer := async.spawn(&sched, consumer)
	async.spawn(&sched, consumer, producer)

	for async.get_pending(&sched) > 0 {
		async.poll(&sched)
		time.sleep(1 * time.Millisecond)
	}
}
```

## Design

The scheduler itself intentionally remains simple.

Tasks suspend themselves through coroutine yields and are resumed through lightweight wakers. External systems can integrate with the scheduler by storing a `Waker` and invoking it once an operation completes.

This design allows asynchronous primitives to be implemented outside the scheduler core.

## Goals

- Small API surface
- Explicit scheduler ownership
- No hidden threads
- No mandatory synchronization primitives
- Easy integration with event-driven systems
- Predictable execution model

## Non-Goals

- Preemptive scheduling
- Work stealing
- Implicit multithreading
- Future/Promise abstractions

## Status

Early development.

The scheduler core and timer support are functional, but APIs may change as additional asynchronous primitives are added.
