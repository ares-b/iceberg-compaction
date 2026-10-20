import shutil
from pathlib import Path

from pyiceberg.io import FileIO

from iceberg_compaction.planning import MIB_TO_BYTES, TrackedFile

COPY_BUFFER_BYTES = 8 * MIB_TO_BYTES

# TODO: engines should read parquet files directly (FileIO or https or whatever)
def stage_files(io: FileIO, files: tuple[TrackedFile, ...], staging_dir: str, name: str) -> list[str]:
    local_paths = []
    for index, tracked_file in enumerate(files):
        local_path = f"{staging_dir}/{name}-{index}.parquet"
        with io.new_input(tracked_file.file.file_path).open() as source, Path(local_path).open("wb") as target:
            shutil.copyfileobj(source, target, COPY_BUFFER_BYTES)
        local_paths.append(local_path)
    return local_paths
