# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

import importlib.util
from typing import Optional
from pjk.base import Source, Pipe, Sink, ParsedToken, UsageError

class UserSourceFactory:
    @staticmethod
    def create(ptok: ParsedToken) -> Optional[Source]:
        script_path = ptok.pre_colon
        try:
            spec = importlib.util.spec_from_file_location("user_source", script_path)
            if spec is None or spec.loader is None:
                raise UsageError(f"Could not load Python file: {script_path}")

            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
        except Exception as e:
            raise UsageError(f"Failed to import {script_path}: {e}")

        for value in vars(module).values():
            if (
                isinstance(value, type)
                and issubclass(value, Source)
                and not issubclass(value, Pipe)
                and not issubclass(value, Sink)
                and value is not Source
                and value.__module__ == module.__name__  # 🧠 only user-defined classes
            ):
                return value(ptok)

        return None
