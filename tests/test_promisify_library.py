import unittest
from src.promisipy import Promise, promisipy, main_event_loop
import time


# Helper functions for test tasks
def sync_task():
    time.sleep(1)
    return "sync_task completed"


def sync_task_with_error():
    raise ValueError("Intentional error for testing")


class TestPromisifyLibrary(unittest.TestCase):

    def test_promise_resolution(self):
        """Test if Promise correctly resolves with a result."""

        promise = Promise(execution=sync_task, mode="threading").start()
        resolution = promise.wait()
        self.assertEqual(resolution.result, "sync_task completed")
        self.assertIsNone(resolution.error)

    def test_promise_error_handling(self):
        """Test if Promise correctly handles and stores exceptions."""

        promise = Promise(execution=sync_task_with_error, mode="threading").start()
        resolution = promise.wait()
        self.assertIsNone(resolution.result)
        self.assertIsInstance(resolution.error, ValueError)

    def test_event_loop_registration_and_unregistration(self):
        """Test if promises are correctly registered and unregistered from the event loop."""

        promise = Promise(execution=sync_task, mode="threading")
        self.assertIn(promise, main_event_loop.promises)
        promise.start().wait()
        self.assertNotIn(promise, main_event_loop.promises)

    def test_promisipy_decorator(self):
        """Test if promisipy decorator correctly creates and manages promises."""

        @promisipy(mode="threading")
        def decorated_task():
            return "decorated_task completed"

        promise = decorated_task().start()
        self.assertIsInstance(promise, Promise)
        resolution = promise.wait()
        self.assertEqual(resolution.result, "decorated_task completed")

    def test_promise_all(self):
        """Test if Promise.all correctly waits for multiple promises to resolve."""

        promise1 = Promise(execution=sync_task, mode="threading").start()
        promise2 = Promise(execution=sync_task, mode="threading").start()
        results = Promise.all([promise1, promise2])
        self.assertEqual(
            [res.result for res in results],
            ["sync_task completed", "sync_task completed"],
        )


if __name__ == "__main__":
    unittest.main()
