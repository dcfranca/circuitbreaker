from abc import abstractclassmethod
from typing import Optional, Tuple, Final, Any, Union, Protocol, Dict
from datetime import datetime, timedelta
from time import sleep
import requests


class CircuitOpenError(Exception):
    """
    This exception will be raised when the circuit breaker is opened
    """
    pass


class Storage(Protocol):
    """
    This is the interface for the storage
    It implements a set of common methods
    """
    @abstractclassmethod
    def get(self, key: str, default: Any = None) -> Any:
        raise NotImplementedError

    @abstractclassmethod
    def set(self, key: str, value: Any, timeout: Optional[int] = None) -> Any:
        raise NotImplementedError

    @abstractclassmethod
    def increment(self, key: str) -> Any:
        raise NotImplementedError


class InMemoryStorage(Storage):
    """
    The actual storage implmentation of the interface
    This can be replaced with a more production ready backend: i.e: Redis
    """
    data: Dict[str, Any] = {}

    def get(self, key: str, default: Any = None) -> Any:
        if key not in self.data:
            return default

        if "expiration" in self.data[key] and self.data[key]["expiration"] < datetime.now():
            return default

        return self.data[key]["value"]

    def set(self, key: str, value: Any, timeout: Optional[int] = None) -> Any:
        self.data[key] = {
            "value": value,
            "expiration": datetime.now() + timedelta(0, timeout) if timeout else None
        }

    def increment(self, key: str) -> Any:
        self.data.setdefault(key, {
            "value": 0
        })
        self.data[key]["value"] += 1
        return self.data[key]["value"]


class CircuitBreaker:
    """
    The circuit breaker is implemented as a Python Context Manager, so it catches exceptions inside the
    context defined
    """
    def __init__(self, _id: str, errors_threshold: int, time_window: int, open_duration, exceptions: Tuple[BaseException]) -> None:
        self.errors_threshold: Final[int] = errors_threshold
        self.time_window: Final[int] = time_window
        self.exceptions: Final[Tuple[BaseException]] = exceptions
        self.open_duration: Final[int] = open_duration
        self.storage: Final[Storage] = InMemoryStorage()
        self._id: Final[str] = _id
        self.keys: Final[Dict[str, str]] = {
            "is_open": f"{self._id}__is_open__",
            "num_errors": f"{self._id}__num_errors__",
        }

    def __enter__(self) -> "CircuitBreaker":
        """
        If the circuit is open, raise an exception with the error of the opened circuit breaker
        This will happen before any requests on the context
        """
        if self.storage.get(self.keys["is_open"], False):
            raise CircuitOpenError(f"{self._id} - Circuit Breaker is opened")

        return self

    def __exit__(self, exc_type: Optional[BaseException], exc_val: Optional[BaseException], traceback: Optional[BaseException]):
        """
        Here we catch defined exceptions and increment the number of errors
        If the errors is over the threshold we open the circuit with an expiration for the open duration
        """
        if exc_type not in self.exceptions:
            return False

        if self.storage.increment(self.keys["num_errors"]) >= self.errors_threshold:
           self.storage.set(self.keys["is_open"], True, self.open_duration)
           self.storage.set(self.keys["num_errors"], 0)

        return True


if __name__ == "__main__":

    settings: Dict[str, Union[int, Tuple[BaseException]]] = {
        "_id": "foobar",
        "errors_threshold": 5,
        "time_window": 20,
        "open_duration": 10,  # seconds
        "exceptions": (requests.Timeout, requests.HTTPError, requests.ConnectionError),
    }

    try:
        for attempt in range(0, 10):
            with CircuitBreaker(**settings):
                print(f"Attempt: {attempt}")
                requests.get("http://localhost:1024")
    except CircuitOpenError:
        print("Raised CircuitOpenError exception")

    print(f"Wait {settings['open_duration']} seconds")
    sleep(settings["open_duration"])

    with CircuitBreaker(**settings):
        print(f"New Attempt")
        requests.get("http://localhost:1024")
