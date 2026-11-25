"""Small atomic JSON persistence helper.

The gateway's correctness depends on every acceptance and receipt being
durable before the caller is told that it was accepted.  A JSON document is
adequate for the single-process edge/central simulation and keeps state easy
to inspect.  Writes use ``os.replace`` so a restart never sees a half-written
document.
"""

from __future__ import annotations

import copy
import json
import os
from pathlib import Path
import tempfile
from typing import Any


class JsonStore:
    def __init__(self, path: str | Path | None = None, *, initial: dict[str, Any]):
        self.path: Path | None
        if path is None:
            self.path = None
            self.data = copy.deepcopy(initial)
            return

        path = Path(path)
        if path.exists() and path.is_dir():
            path = path / "state.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path

        if path.exists():
            with path.open("r", encoding="utf-8") as source:
                self.data = json.load(source)
            if not isinstance(self.data, dict):
                raise ValueError(f"{path}: state root must be an object")
        else:
            self.data = copy.deepcopy(initial)
            self.save()

    def save(self) -> None:
        if self.path is None:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd, temporary_name = tempfile.mkstemp(
            prefix=f".{self.path.name}.", suffix=".tmp", dir=self.path.parent
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as target:
                json.dump(self.data, target, ensure_ascii=False, indent=2, sort_keys=False)
                target.write("\n")
                target.flush()
                os.fsync(target.fileno())
            os.replace(temporary_name, self.path)
        except BaseException:
            try:
                os.unlink(temporary_name)
            except FileNotFoundError:
                pass
            raise

    def reload(self) -> dict[str, Any]:
        if self.path is None:
            return self.data
        with self.path.open("r", encoding="utf-8") as source:
            loaded = json.load(source)
        if not isinstance(loaded, dict):
            raise ValueError(f"{self.path}: state root must be an object")
        self.data = loaded
        return self.data
