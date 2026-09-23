/*
**Cancellation**:

Different kinds of operations can be cancelled in different ways.

When an operation produces a resource (`open`, `accept`, ...), the coroutine is resumed
but the underlying operation is not cancelled immediately. When the callback is called,
it checks whether the operation was cancelled and, if so, releases the resource.

Operations that don't produce resources (`read`, `write`, ...), but may have side effects,
are removed from the event loop when cancelled and the coroutine is resumed.

Cancellation resumes the coroutine first. Whether the underlying operation is removed or
allowed to complete depends on whether it produces a resource.
*/
package async_io

