from __future__ import annotations

from collections import deque
from contextlib import AsyncExitStack
from typing import TYPE_CHECKING, Self

from anyio import TASK_STATUS_IGNORED, create_memory_object_stream, create_task_group, wait_readable
from traitlets import HasTraits, Instance
from zmq import Flag, Frame, PollEvent, Socket, SocketOption

if TYPE_CHECKING:
    from anyio.abc import TaskGroup, TaskStatus


class AsyncSocketReader(HasTraits):
    _task_group: TaskGroup | None = None
    _stack = None
    _socket = Instance(Socket)

    def __init__(self, socket: Socket) -> None:
        self._socket = socket

    async def __aenter__(self) -> Self:
        self._recv_futures = deque()
        self._send_stream, self._receive_stream = create_memory_object_stream[list[Frame]]()
        async with AsyncExitStack() as stack:
            self._task_group = task_group = await stack.enter_async_context(create_task_group())
            await task_group.start(self._start)
            self._stack = stack.pop_all()
        return self

    async def __aexit__(self, exc_type, exc_value, exc_tb):
        if self._stack is not None:
            try:
                await self._stack.__aexit__(exc_type, exc_value, exc_tb)
            finally:
                self._stack = None
        if tg := self._task_group:
            tg.cancel_scope.cancel()

    def __aiter__(self):
        return self._receive_stream


    async def _start(self, *, task_status: TaskStatus[None] = TASK_STATUS_IGNORED):
        task_status.started()
        socket = Socket(self._socket)
        try:
            while True:
                await wait_readable(self._socket)
                while int(self._socket.get(SocketOption.EVENTS)) & PollEvent.POLLIN:
                    result = self._socket.recv_multipart(flags=Flag.DONTWAIT, copy=False)
                    await self._send_stream.send(result)
        finally:
            socket.close(linger=0)
