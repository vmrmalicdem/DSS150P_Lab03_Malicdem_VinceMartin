import os
from pathlib import Path


def atomic_write_bytes(path: Path, data: bytes) -> None:
    """Write via a temp file in the same directory, then os.replace() into the
    final path. os.replace is atomic on both Windows and POSIX, so a reader
    never sees a partially-written file even if the process is killed mid-write."""
    path = Path(path)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with open(tmp_path, "wb") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp_path, path)


def atomic_write_text(path: Path, text: str, encoding: str = "utf-8") -> None:
    atomic_write_bytes(path, text.encode(encoding))


def atomic_write_via(path: Path, write_fn) -> None:
    """For writers that only know how to write to a path (pandas to_parquet,
    to_csv, etc.): write to a .tmp sibling, then atomically replace. write_fn
    receives the temp path and should write the complete file to it."""
    path = Path(path)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    write_fn(tmp_path)
    os.replace(tmp_path, path)
