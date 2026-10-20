from pyiceberg.manifest import FileFormat


class UnsupportedDataFileTypeError(NotImplementedError):
    def __init__(self, file_path: str, file_format: FileFormat):
        self.file_path = file_path
        self.file_format = file_format
        super().__init__(f"Unsupported file format {file_format.value}: {file_path}")


class UnsupportedTableFormatVersionError(NotImplementedError):
    def __init__(self, format_version: int):
        self.format_version = format_version
        super().__init__(f"Unsupported table format version {format_version}")


class RowCountMismatchError(RuntimeError):
    def __init__(self, partition_path: str, rows_read: int, rows_written: int):
        self.partition_path = partition_path
        self.rows_read = rows_read
        self.rows_written = rows_written
        super().__init__(f"Rows written {rows_written} don't match rows read {rows_read} in partition {partition_path!r}")
