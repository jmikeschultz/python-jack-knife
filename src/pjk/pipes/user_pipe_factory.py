# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

# djk/pipes/user_pipe_factory.py

import importlib.util
from typing import Optional
from pjk.base import Pipe, Sink, ParsedToken, UsageError

class UserPipeFactory:
    @staticmethod
    def create(ptok: ParsedToken) -> Optional[Pipe]:
        script_path = ptok.pre_colon

        try:
            # Load module dynamically from script path
            spec = importlib.util.spec_from_file_location("user_pipe", script_path)
            if spec is None or spec.loader is None:
                raise UsageError(f"Could not load Python file: {script_path}")

            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
        except Exception as e:
            raise UsageError(f"Failed to import {script_path}: {e}")

        # Look for exactly one top-level Pipe class that isn't a Sink or base Pipe
        pipe_cls = None
        for value in vars(module).values():
            if (
                isinstance(value, type)
                and issubclass(value, Pipe)
                and not issubclass(value, Sink)
                and value is not Pipe
                and value.__module__ == module.__name__
            ):
                if pipe_cls is not None:
                    raise UsageError(f"Multiple Pipe classes found in {script_path}. Only one is allowed.")
                pipe_cls = value

        if pipe_cls is None:
            return None

        usage = pipe_cls.usage()
        usage.bind(ptok)
        return pipe_cls(ptok, usage)
