#! /usr/bin/env python3

from abc import ABC, abstractmethod
from typing import Any, Protocol


class ExportPlugin(Protocol):
    def process_output(self, data: list[tuple[int, str]]) -> None:
        pass


class CSV:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        print("CSV Output:")
        _, content = zip(*data)
        print(",".join(content))


class JSON:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        print("JSON Output:")
        index, content = zip(*data)
        i: int = 0
        print("{", end="")
        while i < len(index):
            print(f'"item_{index[i]}": "{content[i]}"', end="")
            if i != len(index) - 1:
                print(", ", end="")
            i += 1
        print("}")


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
                self._storage.append((self._rank, str(data)))
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
                f"total {processor._rank} "
                f"items processed, remaining {len(processor._storage)} "
                "on processor"
            )

    def output_pipeline(
            self, nb: int, plugin: ExportPlugin) -> None:
        for processer in self._processors:
            data_to_output: list[tuple[int, str]] = []
            for _ in range(nb):
                try:
                    data_to_output.append(processer.output())
                except IndexError:
                    break
            if data_to_output:
                plugin.process_output(data_to_output)


def main() -> None:
    print("=== Code Nexus - Data Pipeline ===")
    print("Initialize Data Stream...")
    num_proc: NumericProcessor = NumericProcessor()
    txt_proc: TextProcessor = TextProcessor()
    log_proc: LogProcessor = LogProcessor()
    data_stream: DataStream = DataStream()
    print("== DataStream statistics ==")
    data_stream.print_processors_stats()
    print("Registering Processors")
    data_stream.register_processor(num_proc)
    data_stream.register_processor(txt_proc)
    data_stream.register_processor(log_proc)
    test1: list[
        str | int | dict[str, str] |
        list[dict[str, str] | int | str | float]
    ] = [
        'Hello world', [3.14, -1, 2.71],
        [
            {
                'log_level': 'WARNING',
                'log_message': 'Telnet access! Use ssh instead'},
            {
                'log_level': 'INFO',
                'log_message': 'User wil isconnected'}
        ],
        42, ['Hi', 'five']
    ]
    print(f"Send first batch of data on stream: {test1}")
    data_stream.process_stream(test1)
    print("== DataStream statistics ==")
    data_stream.print_processors_stats()
    print("Send 3 processed data from each processor to a CSV plugin:")
    csv_plugin: CSV = CSV()
    data_stream.output_pipeline(3, csv_plugin)
    print("== DataStream statistics ==")
    data_stream.print_processors_stats()
    test2: list[
        str | int | dict[str, str] | list[dict[str, str] | int | str]
    ] = [
        21, [
            'I love AI', 'LLMs are wonderful',
            'Stay healthy'
        ], [{
            'log_level': 'ERROR',
            'log_message': '500 server crash'
        },
            {
            'log_level': 'NOTICE',
                'log_message': 'Certificate expires in 10 days'
        }], [32, 42, 64, 84, 128, 168], 'World hello'
    ]
    print(f"Send another batch of data: {test2}")
    data_stream.process_stream(test2)
    print("== DataStream statistics ==")
    data_stream.print_processors_stats()
    print("Send 5 processed data from each processor to a JSON plugin:")
    json_plugin: JSON = JSON()
    data_stream.output_pipeline(5, json_plugin)
    print("== DataStream statistics ==")
    data_stream.print_processors_stats()


if __name__ == "__main__":
    main()
