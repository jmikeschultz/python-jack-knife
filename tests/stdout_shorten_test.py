# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

from pjk.main import execute_tokens
from pjk.sinks.stdout import shorten_value


def test_shorten_struct_keeps_keys_flat_map_capped():
    """
    Nested structs (dict with nested dict values) keep all keys.
    Flat maps (scalars/lists only) are key-capped.
    """
    obj = {
        "taskName": "Foo",
        "results": [
            {
                "slot": 1,
                "sourcedAds": {
                    "relevantBrandId": None,
                    "adSourcingProductResults": {
                        "a": 1.0,
                        "b": 2.0,
                        "c": 3.0,
                        "d": 4.0,
                        "e": 5.0,
                        "f": 6.0,
                    },
                    "mc3": [],
                    "autoBidCandidates": [
                        {"adId": "1"},
                        {"adId": "2"},
                        {"adId": "3"},
                    ],
                    "adIdMap": {
                        "a": [{"adId": 1}],
                        "b": [{"adId": 2}],
                        "c": [{"adId": 3}],
                        "d": [{"adId": 4}],
                    },
                    "shouldShiftPlacements": False,
                },
            }
        ],
    }
    out = shorten_value(obj, 2, role="record")
    sourced = out["results"][0]["sourcedAds"]
    # struct: all keys present
    assert set(sourced.keys()) == {
        "relevantBrandId",
        "adSourcingProductResults",
        "mc3",
        "autoBidCandidates",
        "adIdMap",
        "shouldShiftPlacements",
    }
    # flat score map capped
    assert sourced["adSourcingProductResults"] == {"a": 1.0, "b": 2.0, "_more": 4}
    # list capped
    assert sourced["autoBidCandidates"] == [{"adId": "1"}, {"adId": "2"}, {"_more": 1}]
    # flat map of lists capped by keys
    assert sourced["adIdMap"] == {
        "a": [{"adId": 1}],
        "b": [{"adId": 2}],
        "_more": 2,
    }


def test_shortlist_elements_keep_all_keys():
    obj = {
        "cars": [
            {"make": "ford", "size": 9, "color": "red", "year": 2010},
            {"make": "bmw", "size": 4, "color": "black", "year": 2012},
            {"make": "honda", "size": 5, "color": "blue", "year": 2015},
        ]
    }
    out = shorten_value(obj, 2, role="record")
    assert out["cars"][0] == {"make": "ford", "size": 9, "color": "red", "year": 2010}
    assert out["cars"][2] == {"_more": 1}


def test_shorten_no_marker_when_fits():
    assert shorten_value({"xs": [1, 2], "meta": {"a": 1}}, 5, role="record") == {
        "xs": [1, 2],
        "meta": {"a": 1},
    }


def test_stdout_shorten_param(monkeypatch, capsys):
    monkeypatch.setenv("PJK_NO_HISTORY", "1")
    execute_tokens(
        [
            "{id:1, labels:{a:1, b:2, c:3}, cars:[{make:'ford', size:9}, {make:'bmw'}, {make:'honda'}]}",
            "-@shorten=2@less=false",
        ]
    )
    out = capsys.readouterr().out
    assert "ford" in out
    assert "honda" not in out
    assert "_more" in out
    assert "size: 9" in out or "size:9" in out.replace(" ", "")
