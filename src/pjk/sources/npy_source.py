# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

import json
from typing import Iterator, Dict, Any

import numpy as np
from pjk.base import Source, NoBindUsage
from pjk.sources.lazy_file import LazyFile
from pjk.sources.format_source import FormatSource
from pjk.log import logger


class NpySource(FormatSource):
    extension = 'npy'

    def __init__(self, lazy_file: LazyFile):
        self.lazy_file = lazy_file
        self.num_vecs = 0

    def __iter__(self) -> Iterator[Dict[str, Any]]:
        """
        Yields one record per embedding row:
            {"emb": "[-0.03026641346514225, -0.09534721076488495, ...]"}
        Notes:
        - Loads with allow_pickle=False for safety.
        - Accepts 1-D or 2-D float arrays. 1-D is treated as a single row.
        - Non-float dtypes are converted to float32 explicitly.
        """
        path = self.lazy_file.path

        try:
            # Use mmap to avoid loading entire array in RAM at once.
            arr = np.load(path, mmap_mode="r", allow_pickle=False)
        except Exception as e:
            logger.error(f"Failed to load .npy file at {path}: {e}")
            return

        if arr.size == 0:
            # Empty array → nothing to yield
            logger.warning(f"Empty embeddings array in {path} (shape={getattr(arr, 'shape', None)})")
            return

        # Normalize shape to 2-D (N, D)
        if arr.ndim == 1:
            arr2d = arr.reshape(1, -1)
        elif arr.ndim == 2:
            arr2d = arr
        else:
            logger.error(f"Unsupported array rank {arr.ndim} in {path}; expected 1-D or 2-D.")
            return

        # Ensure floating dtype (float32 to be consistent with typical pipelines)
        if not np.issubdtype(arr2d.dtype, np.floating):
            try:
                arr2d = arr2d.astype(np.float32, copy=False)
            except Exception as e:
                logger.error(f"Could not cast array to float32 for {path}: {e}")
                return

        # Stream rows; each record has a single field 'emb' that is a JSON string
        # like "[-0.03, 0.12, ...]". We use json.dumps on the Python list to match
        # the desired textual form.
        for i in range(arr2d.shape[0]):
            try:
                row = arr2d[i]
                # Convert to Python list of floats and then to a compact JSON string.
                emb_list = row.tolist()
                emb_str = json.dumps(emb_list, ensure_ascii=False, separators=(",", ":"))
                self.num_vecs += 1
                yield {"emb": emb_str}
            except Exception as e:
                # Log and skip problematic row, mirroring JsonSource’s tolerant behavior.
                logger.warning(
                    f"Skipping invalid embedding at row {i} in {path}: {e}"
                )
