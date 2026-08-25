#! /usr/bin/env python3

from abc import ABC, abstractmethod
from typing import Any

class DataProcessor(ABC):
    def __init__ (self) -> None:
        self._storage: list[tuple[int, str]] = []
        self._rank: int = 0
    


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
