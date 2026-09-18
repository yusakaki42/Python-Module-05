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
                    self._storage.append((
                        self._rank,
                        str(f"{content['log_level']}:"
                            f"{content['log_message']}")
                    ))
                    self._rank += 1
            else:
                self._storage.append((
                    self._rank,
                    str(f"{data['log_level']}: {data['log_message']}")))
                self._rank += 1
        except TypeError as e:
            print(e)


class DataStream():
    def __init__(self) -> None:
        self._processors: list[DataProcessor] = []


    def register_processor(self, proc: DataProcessor) -> None:
        self._processors.append(proc)


    def process_stream(self, stream: list[Any]) -> None:
        for data in stream:
            for processor in self._processors:
                if processor.validate(data):
                    processor.ingest(data)
                    break
            else:
                print(
                    "DataStream error - "
                    f"Can't process element in stream: {data}"
                    )


    def print_processors_stats(self) -> None:
        if not self._processors:
            print("No processor found, no data\n")
        for processor in self._processors:
            name = processor.__class__.__name__.replace(
                'Processor', ' Processor')
            print(
                f"{name}: "
                f"total {processor._rank}"
                f"items processed, remaining {len(processor._storage)} "
                "on processor"
            )


def main() -> None:
    numeric_proc: NumericProcessor = NumericProcessor()
    text_proc = TextProcessor()
    log_proc = LogProcessor()
    print("=== Code Nexus - Data Stream ===")
    print()
    print("Initialize Data Stream...")
    test_class: DataStream = DataStream()
    print("== DataStream statistics ==")
    test_class.print_processors_stats()
    print("Registering Numeric Processor")
    test_class.register_processor(numeric_proc)
    test_data: list[
        str | list[str | float | dict[str, str] | int] |
        int | dict[str, str]
        ] = [
            'Hello world', [3.14, -1, 2.71],
            [{'log_level': 'WARNING',
                'log_message': 'Telnet access! Use ssh instead'},
                {'log_level': 'INFO',
                 'log_message': 'User wil is connected'
                 }], 42, ['Hi', 'five']]
    print(f"Send first batch of data on stream: {test_data}")
    test_class.process_stream(test_data)
    

    # print("=== Code Nexus - Data Processor ===")
    # print()
    # print("Testing Numeric Processor...")
    # test1: list[int | str] = [42, "Hello"]
    # num = NumericProcessor()
    # for example in test1:
    #     print(
    #         f"Trying to validate input '{example}':"
    #         f" {num.validate(example)}"
    #     )
    # test2: str = "foo"
    # print(
    #     "Test invalid ingestion of string "
    #     f"'{test2}' without prior validation:"
    # )
    # num.ingest(test2)

    # test3: list[list | float] = [1, 2, 3, 4, 5]
    # print(f"processing data: {test3}")
    # num.ingest(test3)
    # number: int = 3
    # print(f"Extracting {number} value...")
    # for _ in range(0, number):
    #     key, value = num.output()
    #     print(f"Numeric value {key}: {value}")
    # print()

    # print("Testing Text Processor...")
    # txt = TextProcessor()
    # print(
    #     "Trying to validate input"
    #     f" '{test1[0]}': {txt.validate(test1[0])}"
    # )
    # test4: list[str] = ['Hello', 'Nexus', 'World']
    # print(f"Processing data: {test4}")
    # txt.ingest(test4)
    # number: int = 1
    # print(f"Extracting {number} value...")
    # for _ in range(0, number):
    #     key, value = txt.output()
    #     print(f"Text value {key}: {value}")
    # print()

    # print("Testing Log Processor...")
    # log = LogProcessor()
    # print(
    #     "Trying to validate input "
    #     f"'{test1[1]}': {log.validate(test1[1])}"
    # )
    # test5: list[dict[str, str]] = [
    #     {'log_level': 'NOTICE', 'log_message': 'Connection to server'},
    #     {'log_level': 'ERROR', 'log_message': 'Unauthorized access!!'}
    # ]
    # print(f"Processing data: {test5}")
    # log.ingest(test5)
    # number: int = 2
    # print(f"Extracting {number} values...")
    # for _ in range(0, number):
    #     key, value = log.output()
    #     print(f"Log entry {key}: {value}")


if __name__ == "__main__":
    main()
