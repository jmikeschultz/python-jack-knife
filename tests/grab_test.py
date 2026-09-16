# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

from pjk.main import execute_tokens


def test_grab_cars_drops_siblings():
    execute_tokens(
        [
            "{ferry:'orca', cars:[{make: 'ford', size:9}, {make:'bmw', size:4}]}",
            "grab:cars",
            "expect:[{make: 'ford', size:9}, {make:'bmw', size:4}]",
        ]
    )


def test_grab_dotted_path():
    execute_tokens(
        [
            "{ferry:'orca', fleet:{cars:[{make: 'ford', size:9}, {make:'bmw', size:4}]}}",
            "grab:fleet.cars",
            "expect:[{make: 'ford', size:9}, {make:'bmw', size:4}]",
        ]
    )


def test_grab_concatenates_across_input_records():
    execute_tokens(
        [
            "[{cars:[{make:'ford'}]}, {cars:[{make:'bmw'}, {make:'honda'}]}]",
            "grab:cars",
            "expect:[{make:'ford'}, {make:'bmw'}, {make:'honda'}]",
        ]
    )


def test_grab_missing_path_yields_nothing():
    execute_tokens(
        [
            "{ferry:'orca'}",
            "grab:cars",
            "expect:[]",
        ]
    )


def test_grab_then_where():
    execute_tokens(
        [
            "{ferry:'orca', cars:[{make: 'ford', size:9}, {make:'bmw', size:4}]}",
            "grab:cars",
            "where:f.make=='ford'",
            "expect:[{make: 'ford', size:9}]",
        ]
    )
