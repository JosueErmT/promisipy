# Introduction

This Python project provides a concurrency framework using promises, enabling asynchronous execution of tasks with support for both threading and multiprocessing. It allows for easy task execution management, result handling, and synchronization of concurrent operations without delving into the complexities of threading or multiprocessing modules directly.

## Features

- **Simple API**: Intuitive and easy-to-use API for managing asynchronous task executions.
- **Concurrency Modes**: Supports both threading and multiprocessing to cater to different use cases.
- **Promise-Based**: Utilize promises for handling asynchronous execution results and errors.
- **Event Loop**: Central event loop for managing and synchronizing promise lifecycles.
- **Decorators**: Simplify asynchronous function execution with decorators.

## Installation

This project does not require installation of external packages. Ensure you have Python 3.8 or newer installed on your system, as this project utilizes features introduced in Python 3.8.

To install it, run the following command:

```
pip install promisipy
```

## Usage

### Basic Concepts

1. **Promise**: Represents the eventual completion (or failure) of an asynchronous operation and its resulting value.
2. **Event Loop**: Manages the lifecycle of promises, including registration, execution, and unregistration.
3. **`promisipy` Decorator**: Converts a regular function into one that returns a promise when called, executing the function in a separate thread or process.

### Creating a Promise

To create a promise, instantiate a `Promise` object with a target function. Specify the concurrency mode (`"threading"` or `"multiprocessing"`) according to your needs.

### Waiting for a Promise to Resolve

Use the `wait` method on a promise object to block until the promise has resolved. This method returns a `Promise.Resolution` object containing the result or error.

### Executing Functions Asynchronously

Decorate functions with `@promisipy(mode="threading")` or `@promisipy(mode="multiprocessing")` to run them asynchronously as promises.

## Examples

### Basic Promise Execution

```python
from promisipy import Promise, promisipy

# Function to execute asynchronously
def task():
    return "Result of async task"

# Create and start a promise
promise = Promise(execution=task, mode="threading").start()

# Wait for the promise to resolve
resolution = promise.wait()
print("Result:", resolution.result)
```

### Using `promisipy` Decorator

```python
from promisipy import promisipy

@promisipy(mode="threading")
def async_task():
    return "Result from decorated async task"

# The function now returns a promise
promise = async_task()

# Wait for the promise to resolve and print the result
print("Result:", promise.wait().result)
```

### Waiting for Multiple Promises

```python
from promisipy import Promise, promisipy

# Define multiple tasks
def task1():
    return "Result of task 1"

def task2():
    return "Result of task 2"

# Create promises for each task
promise1 = Promise(execution=task1, mode="threading").start()
promise2 = Promise(execution=task2, mode="threading").start()

# Wait for all promises to resolve
results = Promise.all([promise1, promise2])
print("Results:", [res.result for res in results])
```

This framework is versatile and can be adapted for various asynchronous execution needs, simplifying the handling of concurrent operations in your Python applications.
