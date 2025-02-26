from typing import Callable, List, Any, Literal, Union
import inspect
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

    def then(self, execution):
        if callable(execution):
            argspec = inspect.getfullargspec(execution)
            args = argspec.args
            if inspect.ismethod(execution):                 # if execution is a method
                args = args[1:]                             # then discard the 'self' argument

            if (len(argspec.args) < 1                                                                       # if the amount of args of execution is less than 1
                or len(argspec.args) - (0 if argspec.defaults is None else len(argspec.defaults)) > 1):     # or the amount of required args is more than 1
                raise ValueError("invalid callable")
        else:
            raise TypeError("execution argument is not callable")

        def then():
            resolution = self.wait()
            if resolution.error is None:
                return execution(resolution.result)

        return Promise(then).start()

    def catch(self, execution):
        if callable(execution):
            argspec = inspect.getfullargspec(execution)
            args = argspec.args
            if inspect.ismethod(execution):                 # if execution is a method
                args = args[1:]                             # then discard the 'self' argument

            if (len(argspec.args) < 1                                                                       # if the amount of args of execution is less than 1
                or len(argspec.args) - (0 if argspec.defaults is None else len(argspec.defaults)) > 1):     # or the amount of required args is more than 1
                raise ValueError("invalid callable")
        else:
            raise TypeError("execution argument is not callable")

        def catch():
            resolution = self.wait()
            if resolution.error is not None:
                return execution(resolution.error)

        return Promise(catch).start()

    @staticmethod
    def all(promises: List["Promise"]):
        return [promise.wait() for promise in promises]


def promisipy(
    mode: Literal["threading", "multiprocessing"] = "threading",
    event_loop=main_event_loop,
    autostart=False
):
    def decorator(fn):
        @wraps(fn)
        def wrapped(*args, **kwargs):
            p = Promise(
                lambda: fn(*args, **kwargs),
                mode=mode,
                event_loop=event_loop,
            )
            if autostart:
                p = p.start()

            return p

        return wrapped

    return decorator
