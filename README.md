# ringbuffer
Yet another ringbuffer implementation.

Based on [smallnest/ringbuffer](https://github.com/smallnest/ringbuffer)

Consumes two times more memory than normal ring buffer
but unlike normal ring buffer you can access continuous (you don't have to handle wrapping) underlying buffer
without memory allocation/copy.
