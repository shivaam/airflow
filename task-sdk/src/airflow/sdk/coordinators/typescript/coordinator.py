# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""TypeScript runtime coordinator that launches a Node.js subprocess for task execution."""

from __future__ import annotations

import contextlib
import os
import pathlib
import stat
from typing import TYPE_CHECKING

import attrs
import structlog

from airflow.sdk.coordinators._subprocess import SubprocessCoordinator

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator

    from structlog.typing import FilteringBoundLogger

    from airflow.sdk.execution_time.workloads.task import TaskInstanceDTO

log: FilteringBoundLogger = structlog.get_logger(logger_name="coordinators.typescript")

# Must match SUPERVISOR_API_VERSION in ts-sdk/src/generated/supervisor.ts.
# Hardcoded for the initial release while Airflow and the TypeScript SDK
# share one source tree. When the npm package has an independent release
# cadence, this should be read from bundle metadata instead.
_SCHEMA_VERSION = "2026-06-16"


def _find_bundles(items: Iterable[pathlib.Path]) -> Iterator[pathlib.Path]:
    """
    Yield ``.mjs`` files under *items*, descending into directories.

    Directories are deduplicated by ``(st_dev, st_ino)`` to prevent
    symlink-loop recursion — same pattern as JavaCoordinator.
    """
    seen_dirs: set[tuple[int, int]] = set()
    yield from _walk_bundles(items, seen_dirs)


def _walk_bundles(items: Iterable[pathlib.Path], seen_dirs: set[tuple[int, int]]) -> Iterator[pathlib.Path]:
    for item in items:
        try:
            st = item.stat()
        except OSError:
            continue
        if stat.S_ISDIR(st.st_mode):
            key = (st.st_dev, st.st_ino)
            if key in seen_dirs:
                log.debug("Skipping already-visited directory", path=item)
                continue
            seen_dirs.add(key)
            with contextlib.suppress(OSError):
                yield from _walk_bundles(item.iterdir(), seen_dirs)
        elif stat.S_ISREG(st.st_mode) and item.suffix == ".mjs":
            yield item


def _convert_bundles_root(
    value: None | str | os.PathLike[str] | pathlib.Path | list[str | os.PathLike[str] | pathlib.Path],
) -> list[pathlib.Path]:
    if value is None:
        return []
    if isinstance(value, (str, os.PathLike, pathlib.Path)):
        return [pathlib.Path(value).expanduser()]
    return [pathlib.Path(v).expanduser() for v in value]


@attrs.define(kw_only=True)
class TypescriptCoordinator(SubprocessCoordinator):
    """
    Coordinator that launches a Node.js subprocess for task execution.

    Configuration is taken from the ``[sdk] coordinators`` entry::

        {
            "name": "ts",
            "classpath": "airflow.sdk.coordinators.typescript.TypescriptCoordinator",
            "kwargs": {
                "bundles_root": ["~/airflow/ts-bundles"],
            },
        }

    :param node_executable: Path to the ``node`` command (defaults to
        ``"node"``, which relies on ``$PATH``).
    :param bundles_root: A list of directories scanned for ``.mjs`` bundles
        produced by esbuild (or any ESM bundler).
    :param task_startup_timeout: Maximum time the coordinator waits for a task
        process to connect, in seconds.  The default is 10 seconds.

    The coordinator scans *bundles_root* for ``.mjs`` files.  When a single
    bundle is found it is used for all tasks; when multiple bundles exist the
    coordinator raises an error. Deploy one bundle per configured coordinator
    root for deterministic behaviour.

    Bundles are plain ESM files produced by esbuild (or any bundler that
    emits a single ``.mjs``). No special trailer or metadata is required —
    the coordinator launches ``node <bundle> --comm=... --logs=...``
    directly.
    """

    node_executable: str = "node"
    bundles_root: list[pathlib.Path] = attrs.field(
        converter=_convert_bundles_root,
        validator=attrs.validators.min_len(1),
    )

    def _find_bundle(self) -> pathlib.Path:
        """Return the only ``.mjs`` bundle found under *bundles_root*."""
        bundle: pathlib.Path | None = None
        for candidate in _find_bundles(self.bundles_root):
            if bundle is not None:
                raise ValueError(
                    "Multiple .mjs bundles found in bundles_root; configure one "
                    "TypescriptCoordinator per bundle root"
                )
            bundle = candidate
        if bundle is not None:
            log.debug("Bundle located", path=bundle)
            return bundle
        raise FileNotFoundError(
            f"No .mjs bundle found in {os.pathsep.join(os.fspath(p.resolve()) for p in self.bundles_root)}"
        )

    def _build_execute_task_command(self, *, what: TaskInstanceDTO) -> tuple[list[str], str | None]:
        bundle = self._find_bundle()
        command = [self.node_executable, str(bundle)]
        return command, _SCHEMA_VERSION
