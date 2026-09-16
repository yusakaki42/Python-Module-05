#! /usr/bin/env python3

from abc import ABC, abstractmethod
from typing import Any


class DataProcessor(ABC):
    def __init__(self) -> None:
        self._storage: list[tuple[int, str]] = []
        self._rank: int = 0

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    @abstractmethod
    def ingest(self, data: Any) -> None:
        pass

    def output(self) -> tuple[int, str]:
        return self._storage.pop(0)


class NumericProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, (int, float)):
            return True
        elif isinstance(data, list):
            for content in data:
                if not isinstance(content, (int, float)):
                    return False
            return True
        else:
            return False

    def ingest(self, data: int | float | list[int | float]) -> None:
        try:
            if not self.validate(data):
                raise TypeError("Got exception: Improper numeric data")
            if isinstance(data, list):
                for content in data:
                    self._storage.append((self._rank, str(content)))
                    self._rank += 1
            else:
                self._storage.append((self._rank, str(data)))
                self._rank += 1
        except TypeError as e:
            print(e)


class TextProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, str):
            return True
        elif isinstance(data, list):
            for content in data:
                if not isinstance(content, str):
                    return False
                return True
            else:
                return False

    def ingest(self, data: str | list[str]) -> None:
        try:
            if not self.validate(data):
                raise TypeError("Got exception: Improper text data")
            if isinstance(data, list):
                for content in data:
                    self._storage.append((self._rank, str(content)))
                    self._rank += 1
            else:
                self._storage.append(self._rank, str(data))
                self._rank += 1
        except TypeError as e:
            print(e)


class LogProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, dict):
            return True
        elif isinstance(data, list):
            for content in data:
                if not isinstance(content, dict):
                    return False
                return True
        else:
            return False

    def ingest(self, data: dict[str, str] | list[dict[str, str]]) -> None:
        try:
            if not self.validate(data):
                raise TypeError("Got exception: Improper log data")
            if isinstance(data, list):
                for content in data:
                    self._storage.append(
                        self._rank,
                        str(f"{content['log_level']}:"
                            f"{content['log_message']}")
                    )
                    self._rank += 1
            else:
                self._storage.append((
                    self._rank,
                    str(f"{data['log_level']}: {data['log_message']}")))
                self._rank += 1
        except TypeError as e:
            print(e)


def main() -> None:
    print("=== Code Nexus - Data Processor ===")
    print()
    print("Testing Numeric Processor...")
    test1: list[int | str] = [42, "Hello"]
    num = NumericProcessor()
    for example in test1:
        print(
            f"Trying to validate input '{example}':"
            f" {num.validate(example)}"
        )
    test2: str = "foo"
    print(
        "Test invalid ingestion of string "
        f"'{test2}' without prior validation:"
    )
    num.ingest(test2)

    test3: list[list | float] = [1, 2, 3, 4, 5]
    print(f"processing data: {test3}")
    num.ingest(test3)
    number: int = 3
    print(f"Extracting {number} value...")
    for i in range(0, number):
        key, value = num.output()
        print(f"Numeric value {i}: {number}")


if __name__ == "__main__":
    main()
