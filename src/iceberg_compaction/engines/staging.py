import shutil
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from pyiceberg.io import FileIO

from iceberg_compaction.planning import MIB_TO_BYTES, TrackedFile

COPY_BUFFER_BYTES = 8 * MIB_TO_BYTES


# TODO: engines should read parquet files directly (FileIO or https or whatever)
def stage_files(
    io: FileIO, files: tuple[TrackedFile, ...], staging_dir: str, name: str, max_concurrent_downloads: int = 8
) -> list[str]:
    local_paths = [f"{staging_dir}/{name}-{index}.parquet" for index in range(len(files))]

    def download(tracked_file: TrackedFile, local_path: str) -> None:
        with io.new_input(tracked_file.file.file_path).open() as source, Path(local_path).open("wb") as target:
            shutil.copyfileobj(source, target, COPY_BUFFER_BYTES)

    with ThreadPoolExecutor(max_concurrent_downloads) as executor:
        list(executor.map(download, files, local_paths))
    return local_paths
