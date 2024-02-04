# Introduction

This Python library provides a flexible and easy-to-use asynchronous programming model based on the concept of promises, similar to those found in JavaScript. It supports both threading and multiprocessing for executing tasks concurrently, allowing users to write efficient and non-blocking code that can scale across multiple cores or simply leverage threads for I/O-bound tasks. At the core of the library are two main components: EventLoop and Promise.

EventLoop: Manages a collection of promises, facilitating the registration of new promises and waiting for their completion. It acts as the central hub through which all asynchronous activities are coordinated.

Promise: Represents a single asynchronous operation. A promise can be executed in either a threading or multiprocessing context, depending on the task's requirements. It encapsulates the task execution, result storage, and error handling. Promises can be waited on for completion, and their results retrieved once finished. Additionally, the library provides a mechanism to wait for multiple promises simultaneously, simplifying the handling of concurrent operations.

This library is designed to be lightweight and straightforward, making it suitable for a wide range of applications, from IO-bound tasks to CPU-heavy processes, by abstracting the complexity of managing threads and processes behind a simple API.

# Example of usage

```python
from py_promise import Promise
import requests
from pprint import pprint


def get_rnm_info_from_id(rnm_id: str) -> dict:
    result = requests.get(f"https://rickandmortyapi.com/api/character/{rnm_id}")
    data = result.json()
    name = data["name"]
    origin_url = data["origin"]["url"]
    location_url = data["location"]["url"]

    origin_promise = Promise(lambda: requests.get(origin_url).json()).start()
    location_promise = Promise(lambda: requests.get(location_url).json()).start()

    origin_resolution, location_resolution = Promise.all(
        [origin_promise, location_promise]
    )

    return {
        "id": rnm_id,
        "name": name,
        "origin": origin_resolution.result["name"],
        "location": location_resolution.result["name"],
    }


def main():
    promises = [
        Promise(lambda i=i: get_rnm_info_from_id(i), mode="multiprocessing").start()
        for i in range(1, 100)
    ]
    profiles = [profile_resultion.result for profile_resultion in Promise.all(promises)]
    pprint(profiles)


if __name__ == "__main__":
    main()
```