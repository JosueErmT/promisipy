from typing import Callable, List, Any, Literal, Union
import threading
import multiprocessing
from queue import Empty
from functools import wraps


class EventLoop:
    promises: List["Promise"]

    def __init__(self):
        self.promises = []

    def register(self, promise: "Promise"):
        self.promises.append(promise)

    def unregister(self, promise: "Promise"):
        self.promises = [p for p in self.promises if p != promise]

    def wait(self):
        for promise in list(self.promises):
            if promise.status == Promise.Status.STARTED:
                promise.wait()


main_event_loop = EventLoop()


class Promise:
    class Status:
        CREATED = "created"
        STARTED = "started"
        FINISHED = "finished"

    class Resolution:
        result: Any = None
        error: Any = None

        def __init__(self, result=None, error=None):
            self.result = result
            self.error = error

    task: Union[threading.Thread, multiprocessing.Process]
    resolution: "Promise.Resolution"
    event_loop: EventLoop
    status: "Promise.Status"
    _metadata: dict

    def __init__(
        self,
        execution,
        mode: Literal["threading", "multiprocessing"] = "threading",
        event_loop=main_event_loop,
    ):
        self.status = self.Status.CREATED
        self.resolution = self.Resolution()
        self.event_loop = event_loop
        self._metadata = {"queue": multiprocessing.Queue()}
        self.event_loop.register(self)

        if mode == "threading":
            self.task = threading.Thread(
                target=self._execution_wrapper,
                args=(execution, self._metadata["queue"]),
            )
        elif mode == "multiprocessing":
            self.task = multiprocessing.Process(
                target=self._execution_wrapper,
                args=(execution, self._metadata["queue"]),
            )

    def _execution_wrapper(self, execution: Callable, queue: multiprocessing.Queue):
        try:
            result = execution()
            if type(result) is Promise:
                result = result.wait()
            queue.put((result, None))
        except Exception as e:
            queue.put((None, e))

    def start(self):
        if self.status != self.Status.CREATED:
            return self

        self.status = self.Status.STARTED
        self.task.start()
        return self

    def wait(self):
        if self.status == self.Status.CREATED:
            raise Exception("Promise has not been started.")

        if self.status == self.Status.STARTED:
            self.task.join()

            try:
                result, error = self._metadata["queue"].get()
                self.resolution.result = result
                self.resolution.error = error
            except Empty:
                pass

            self.status = Promise.Status.FINISHED
            self.event_loop.unregister(self)

        return self.resolution

    @staticmethod
    def all(promises: List["Promise"]):
        return [promise.wait() for promise in promises]


def promisipy(
    mode: Literal["threading", "multiprocessing"] = "threading",
    event_loop=main_event_loop,
):
    def decorator(fn):
        @wraps(fn)
        def wrapped(*args, **kwargs):
            return Promise(
                lambda: fn(*args, **kwargs),
                mode=mode,
                event_loop=event_loop,
            )

        return wrapped

    return decorator
