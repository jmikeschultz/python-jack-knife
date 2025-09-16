# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

from abc import ABC, abstractmethod
from typing import IO

class LazyFile(ABC):
    @abstractmethod
    def open(self) -> IO[str]:
        """Open and return a text-mode file-like object."""
        pass

    @abstractmethod
    def name(self) -> str:
        """Return a descriptive identifier (e.g. path or URI)."""
        pass
