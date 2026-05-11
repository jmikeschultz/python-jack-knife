# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

# pjk/pipes/ddiff.py

import json
from itertools import zip_longest

from deepdiff import DeepDiff

from pjk.components import Pipe
from pjk.usage import Usage, ParsedToken
from pjk.progress import papi

_PAD = object()

_DDIFF_OLD_NEW_KEYS = (
    ("old_value", "left_value"),
    ("new_value", "right_value"),
    ("old_type", "left_type"),
    ("new_type", "right_type"),
)


def _diff_left_right_labels(obj):
    """Map DeepDiff old/new keys to left/right (matches stream argument order)."""
    if isinstance(obj, dict):
        out = {}
        for k, v in obj.items():
            for old_k, new_k in _DDIFF_OLD_NEW_KEYS:
                if k == old_k:
                    k = new_k
                    break
            out[k] = _diff_left_right_labels(v)
        return out
    if isinstance(obj, list):
        return [_diff_left_right_labels(x) for x in obj]
    return obj


class DdiffPipe(Pipe):
    arity = 2

    @classmethod
    def usage(cls):
        u = Usage(
            name="ddiff",
            desc=(
                "Lockstep deep diff of two record streams."
            ),
            component_class=cls,
        )
        u.def_syntax(
            "pjk <left_source> <right_source> ddiff ..."
        )
        u.def_param(
            name="ignore_order",
            usage="DeepDiff ignore_order (lists and sets)",
            valid_values={"true", "false"},
            default="false",
        )
        u.def_param(
            name="omit_equal",
            usage="Suppress output when the two records are deeply equal",
            valid_values={"true", "false"},
            default="false",
        )
        u.def_param(
            name="significant_digits",
            usage="DeepDiff significant_digits for numeric comparisons",
            is_num=True,
            default=None,
        )
        ferry_ford = "[{ferry:'orca', cars:[{make: 'ford', size:9}]}]"
        ferry_bmw = "[{ferry:'orca', cars:[{make: 'bmw', size:4}]}]"
        ferry_ford_rec = {"ferry": "orca", "cars": [{"make": "ford", "size": 9}]}
        ferry_bmw_rec = {"ferry": "orca", "cars": [{"make": "bmw", "size": 4}]}
        u.def_example(
            expr_tokens=[ferry_ford, ferry_ford, "ddiff"],
            expect=json.dumps(
                [{"left": ferry_ford_rec, "right": ferry_ford_rec, "diff": {}}],
                separators=(",", ":"),
            ),
        )
        u.def_example(
            expr_tokens=[ferry_ford, ferry_bmw, "ddiff"],
            expect=json.dumps(
                [
                    {
                        "left": ferry_ford_rec,
                        "right": ferry_bmw_rec,
                        "diff": _diff_left_right_labels(
                            json.loads(
                                DeepDiff(
                                    ferry_ford_rec, ferry_bmw_rec
                                ).to_json()
                            )
                        ),
                    }
                ],
                separators=(",", ":"),
            ),
        )
        return u

    def __init__(self, ptok: ParsedToken, usage: Usage):
        super().__init__(ptok, usage)
        self.recs_in = papi.get_counter(self, "recs_in", display=False)
        self.recs_out = papi.get_counter(self, "recs_out")

    def reset(self):
        pass

    @staticmethod
    def _truthy(param) -> bool:
        if param is None:
            return False
        return str(param).lower() == "true"

    def __iter__(self):
        ignore_order = self._truthy(self.usage.get_param("ignore_order"))
        omit_equal = self._truthy(self.usage.get_param("omit_equal"))
        sig = self.usage.get_param("significant_digits")

        dd_kwargs = {}
        if ignore_order:
            dd_kwargs["ignore_order"] = True
        if sig is not None:
            dd_kwargs["significant_digits"] = sig

        for left_rec, right_rec in zip_longest(self.left, self.right, fillvalue=_PAD):
            self.recs_in.increment()
            if left_rec is _PAD:
                left_rec = {}
            if right_rec is _PAD:
                right_rec = {}

            d = DeepDiff(left_rec, right_rec, **dd_kwargs)
            # Normalize to JSON-native dict/list (to_dict() may use e.g. SetOrdered).
            diff_map = _diff_left_right_labels(json.loads(d.to_json()))
            if omit_equal and not diff_map:
                continue
            self.recs_out.increment()
            yield {"left": left_rec, "right": right_rec, "diff": diff_map}
