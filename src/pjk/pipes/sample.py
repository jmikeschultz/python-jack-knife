# SPDX-License-Identifier: Apache-2.0
# Copyright 2025

from __future__ import annotations

import random
from typing import Iterable, List, Optional

from pjk.base import Pipe, ParsedToken, Usage, TokenError


class SamplePipe(Pipe):
    """
    Reservoir-style sampler:
      if recno < nsamples: A[recno] = rec
      else:
        i = p * recno   (p ~ Uniform[0,1))
        if i < nsamples: A[i] = rec

    Exhausts all input, then yields the sampled records.
    """

    @classmethod
    def usage(cls) -> Usage:
        u = Usage(
            name="sample",
            desc="Randomly sample a fixed number of records from the input (single-threaded).",
            component_class=cls,
        )
        # Single positional arg: count
        u.def_arg("count", "Number of records to sample (integer ≥ 0)", is_num=True)
        return u

    def __init__(self, ptok: ParsedToken, usage: Usage):
        super().__init__(ptok, usage)
        self.nsamples: int = int(usage.get_arg("count"))
        if self.nsamples < 0:
            raise Exception("sample: count must be ≥ 0")

        # Preallocate reservoir
        self._A: List[Optional[dict]] = [None] * self.nsamples if self.nsamples > 0 else []
        self._seen: int = 0  # recno (1-based in algo below)

    def reset(self):
        # New instance per run; nothing to reset between drains.
        pass

    def __iter__(self) -> Iterable[dict]:
        # Fill/replace in the reservoir according to the provided algorithm
        for rec in self.left:
            self._seen += 1
            if self.nsamples == 0:
                continue  # nothing to store
            if self._seen <= self.nsamples:
                # A[recno] = rec   (using 0-based index: recno-1)
                self._A[self._seen - 1] = rec
            else:
                # i = p * recno, p ~ U[0,1)
                i = int(random.random() * self._seen)  # 0 .. recno-1
                if i < self.nsamples:
                    self._A[i] = rec

        # Emit the sampled records (compact out any None if input < nsamples)
        for rec in self._A:
            if rec is not None:
                yield rec
