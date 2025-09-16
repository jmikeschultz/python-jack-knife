# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

import importlib.util
from typing import Optional
from pjk.base import Source, Sink, UsageError, ParsedToken

class UserSinkFactory:
    @staticmethod
    def create_from_path(ptok: ParsedToken) -> Optional[Sink]:
        script_path = ptok.pre_colon
        try:
            spec = importlib.util.spec_from_file_location("user_sink", script_path)
            if spec is None or spec.loader is None:
                raise UsageError(f"Could not load Python file: {script_path}")

            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
        except Exception as e:
            raise UsageError(f"Failed to import {script_path}: {e}")

        for value in vars(module).values():
            if (
                isinstance(value, type)
                and issubclass(value, Sink)
                and value is not Sink
                and value.__module__ == module.__name__
            ):
                usage = value.usage()
                usage.bind(ptok)

                return value(ptok, usage)

        return None

    @classmethod
    def create(cls, ptok: ParsedToken) -> Sink:
        if ptok.pre_colon.endswith('.py'):
            sink = cls.create_from_path(ptok)
            if sink:
                return sink

        