"""The Parquet lake on HDFS, reached over WebHDFS.

WebHDFS rather than libhdfs keeps this image free of a JVM — see
docs/architecture.md. The practical consequence is that a write is a two-step
exchange with a redirect, which is why the cluster must be configured to return
DataNode *hostnames*; `fsspec` follows the redirect for us.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal

import fsspec
import polars as pl

from smart_agri.utils import get_logger

if TYPE_CHECKING:
    from collections.abc import Sequence

    from smart_agri.config import HdfsSettings, LakeZone

logger = get_logger(__name__)

PARQUET_COMPRESSION: Literal["zstd"] = "zstd"
"""Better ratio than snappy at comparable speed, and read support is universal."""


class HdfsStore:
    """Reads and writes Parquet datasets in the lake."""

    def __init__(self, settings: HdfsSettings) -> None:
        self._settings = settings
        self._fs: fsspec.AbstractFileSystem | None = None

    @property
    def fs(self) -> fsspec.AbstractFileSystem:
        """Lazily-opened filesystem.

        Deferred so constructing a store — which pipelines do at import time in
        some tests — never opens a connection.
        """
        if self._fs is None:
            self._fs = fsspec.filesystem(
                "webhdfs",
                host=self._settings.namenode_host,
                port=self._settings.namenode_http_port,
                user=self._settings.user,
            )
        return self._fs

    # -- paths ---------------------------------------------------------------
    def zone_path(self, zone: LakeZone, *parts: str) -> str:
        """Absolute lake path. Delegates so path rules live in one place."""
        return self._settings.zone_path(zone, *parts)

    # -- writes --------------------------------------------------------------
    def write_parquet(self, frame: pl.DataFrame, path: str) -> int:
        """Write a DataFrame as a single Parquet file, creating parent dirs.

        Returns the number of rows written.
        """
        parent = path.rsplit("/", 1)[0]
        self.fs.makedirs(parent, exist_ok=True)

        with self.fs.open(path, "wb") as handle:
            frame.write_parquet(handle, compression=PARQUET_COMPRESSION)

        logger.info("hdfs_write", path=path, rows=frame.height)
        return frame.height

    # -- reads ---------------------------------------------------------------
    def read_parquet(self, path: str) -> pl.DataFrame:
        """Read a single Parquet file."""
        with self.fs.open(path, "rb") as handle:
            return pl.read_parquet(handle)

    def read_parquet_dir(self, path: str, *, missing_ok: bool = False) -> pl.DataFrame:
        """Read every Parquet file under a path and concatenate them.

        Files are read in sorted order so the result is deterministic — which
        matters because downstream aggregate output is compared in tests.

        Args:
            path: Directory to read, recursively.
            missing_ok: When True, an absent or empty directory yields an empty
                frame instead of raising. Used by pipelines that legitimately
                run on a date with no data.
        """
        if not self.exists(path):
            if missing_ok:
                logger.warning("hdfs_read_missing", path=path)
                return pl.DataFrame()
            msg = f"lake path does not exist: {path}"
            raise FileNotFoundError(msg)

        files = sorted(self.list_files(path, suffix=".parquet"))
        if not files:
            if missing_ok:
                logger.warning("hdfs_read_empty", path=path)
                return pl.DataFrame()
            msg = f"no parquet files under: {path}"
            raise FileNotFoundError(msg)

        frames = [self.read_parquet(file) for file in files]
        # `vertical_relaxed` tolerates a column that widened between partitions,
        # e.g. an all-null Int64 written before any real value existed.
        result = pl.concat(frames, how="vertical_relaxed")
        logger.info("hdfs_read", path=path, files=len(files), rows=result.height)
        return result

    # -- listing and removal -------------------------------------------------
    def exists(self, path: str) -> bool:
        return bool(self.fs.exists(path))

    def list_files(self, path: str, suffix: str | None = None) -> Sequence[str]:
        """All files beneath a path, recursively, optionally filtered by suffix."""
        if not self.exists(path):
            return []
        found = self.fs.find(path)
        paths = [self._absolute(entry) for entry in found]
        return [p for p in paths if suffix is None or p.endswith(suffix)]

    def makedirs(self, path: str) -> None:
        self.fs.makedirs(path, exist_ok=True)

    def remove(self, path: str, *, recursive: bool = True) -> None:
        """Delete a path if it exists.

        Used to make a partition write idempotent: the target is cleared before
        it is rewritten, so a re-run replaces rather than duplicates.
        """
        if self.exists(path):
            self.fs.rm(path, recursive=recursive)
            logger.info("hdfs_remove", path=path)

    @staticmethod
    def _absolute(entry: str) -> str:
        """Normalise an fsspec listing entry to an absolute HDFS path.

        WebHDFS listings can come back without the leading slash depending on
        the call, and every caller here expects an absolute path.
        """
        return entry if entry.startswith("/") else f"/{entry}"
