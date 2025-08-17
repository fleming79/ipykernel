# Kernel module

::: async_kernel.kernel

# Kernel messaging

`shell` and `control` messages are processed with the [_receive_msg_loop][async_kernel.Kernel._receive_msg_loop] running running in event loops in separate threads. The `shell` thread the "MainThread" and `control`thread is provided by a *protected* [Caller][async_kernel.Caller] thread named "CallerThread".

The 

::: async_kernel.Kernel._receive_msg_loop

The 

::: async_kernel.Kernel._shell_execute_request_queue
