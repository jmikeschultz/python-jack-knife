# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

from abc import ABC, abstractmethod
from typing import Any, Optional, List
from pjk.usage import Usage, NoBindUsage, ParsedToken
    
# mixin
class KeyedSource(ABC):
    @classmethod
    def usage(cls):
        return Usage(
            name=cls.__name__,
            desc=f"{cls.__name__} component"
        )
    
    @abstractmethod
    def lookup(self, left_rec) -> Optional[dict]:
        """Return the record associated with the given key, or None."""
        pass

    def get_unlookedup_records(self) -> List[Any]:
        # for outer join
        pass

    def deep_copy(self):
        return None

class Source(ABC):
    @classmethod
    def usage(cls):
        return NoBindUsage(
            name=cls.__name__,
            desc=f"{cls.__name__} component",
            component_class=cls
        )

    def __init__(self, root = None):
        self.root = root

    @abstractmethod
    def __iter__(self):
        pass

    def __next__(self):
        # lazily create an internal iterator the first time next() is called
        if not hasattr(self, "_iter"):
            self._iter = iter(self)
        return next(self._iter)

    def deep_copy(self):
        return None  # Default: not copyable unless overridden
    
    def close(self):
        pass
    
    def _get_sources(self, source_list: list):
        pass
    
class Pipe(Source):
    arity: int = 1
    
    def __init__(self, ptok: ParsedToken, usage: Usage, root = None):
        self.root = root
        self.ptok = ptok
        self.usage = usage
        self.left = None  # left source for convience
        self.right = None # right source for convience
        self.inputs: List[Source] = []

    def add_source(self, source: Source) -> None:
        self.inputs.append(source)
        # first two are assigned left, right
        if self.left is None:
            self.left = source
        elif self.right is None:
            self.right = self.left
            self.left = source

    def reset(self):
        pass  # optional hook

    def deep_copy(self) -> Optional["Pipe"]:
        return None
    
    def _get_sources(self, source_list: list):
        for ix in self.inputs:
            source_list.append(ix)
            ix._get_sources(source_list)

class DeepCopyPipe(Pipe):
    def deep_copy(self):
        """
        Generic deep_copy: clone left source, re-instantiate
        this pipe class with the same ptok/usage, and attach.
        """
        source_clone = self.left.deep_copy()
        if not source_clone:
            return None

        # re-instantiate using the actual subclass
        pipe = type(self)(self.ptok, self.usage, self) # this self is the root
        pipe.add_source(source_clone)
        return pipe

class Sink(ABC):
    @classmethod
    def usage(cls):
        return NoBindUsage(
            name=cls.__name__,
            desc=f"{cls.__name__} component",
            component_class=cls
        )
    
    def __init__(self, ptok: ParsedToken, usage: Usage, root = None):
        self.root = root
        self.ptok = ptok
        self.usage = usage

    def drain(self):
        self.process()
        self.close()

        # get all inputs in the execution chain for closing
        inputs = [self.input]
        self.input._get_sources(inputs)
        for input in inputs:
            input.close()

    # optional
    def close(self):
        pass

    def add_source(self, source: Source) -> None:
        self.input = source
        
    @abstractmethod
    def process(self) -> None:
        pass

    def deep_copy(self):
        return None
