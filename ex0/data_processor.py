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



def test_numeric() -> None:
    print("Testing Numeric Processor...")
    numeric: NumericProcessor = NumericProcessor()

def main() -> None:
    print("=== Code Nexus - Data Processor ===")
    test_numeric()
    test_text()
    test_log()

if __name__ == "__main__":
    main()
