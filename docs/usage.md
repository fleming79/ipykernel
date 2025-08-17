# Usage

Async kernel is asynchronous by design, shell messaging runs in a loop in the main thread

## Tips

<div class="annotate" markdown>

- Use [async_kernel.Caller.call_soon][] or [async_kernel.Caller.call_later][] to run code in tasks to support either backend.(1)
- Use [anyio](https://anyio.readthedocs.io) or async functions corresponding to the anyio backend(2) freely in the main thread.
- Use [async_kernel.Caller.start_new][] to start a thread with the opposite backend.
- Start a new Caller if there are functions require the opposite asynchronous backend.  

</div>
1. Caller provides methods for thread safe scheduling and awaiting the result using [`Futures`][async_kernel.caller.Future])
    1. Use `Caller.get_instance()` to get the `Caller` for the main thread.
    2. Use `Caller()` to get the `Caller` for the current thread.

2. Async-kernel runs in anyio. 
3. 