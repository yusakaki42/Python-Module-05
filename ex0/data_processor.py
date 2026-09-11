#! /usr/bin/env python3

from abc import ABC, abstractmethod
from typing import Any

class DataProcessor(ABC):
    def __init__ (self) -> None:
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
                if not self.validata(data):
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


# def test_numeric() -> None:
#     print("Testing Numeric Processor...")
#     numeric: NumericProcessor = NumericProcessor()

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

if __name__ == "__main__":
    main()
