from pjk.main import execute_tokens


def test_ddiff_equal_pair():
    execute_tokens(
        [
            "[{a:1}]",
            "[{a:1}]",
            "ddiff",
            "expect:[{left:{a:1},right:{a:1},diff:{}}]",
        ]
    )


def test_ddiff_value_changed():
    execute_tokens(
        [
            "[{a:1}]",
            "[{a:2}]",
            "ddiff",
            "expect:[{left:{a:1},right:{a:2},diff:{values_changed:{\"root['a']\":{right_value:2,left_value:1}}}}]",
        ]
    )


def test_ddiff_longer_left_pads_empty_right():
    execute_tokens(
        [
            "[{a:1},{a:2}]",
            "[{a:1}]",
            "ddiff",
            "expect:["
            "{left:{a:1},right:{a:1},diff:{}},"
            "{left:{a:2},right:{},diff:{dictionary_item_removed:[\"root['a']\"]}}"
            "]",
        ]
    )


def test_ddiff_longer_right_pads_empty_left():
    execute_tokens(
        [
            "[{a:1}]",
            "[{a:1},{b:2}]",
            "ddiff",
            "expect:["
            "{left:{a:1},right:{a:1},diff:{}},"
            "{left:{},right:{b:2},diff:{dictionary_item_added:[\"root['b']\"]}}"
            "]",
        ]
    )


def test_ddiff_omit_equal():
    execute_tokens(
        [
            "[{a:1},{a:2}]",
            "[{a:1},{a:3}]",
            "ddiff@omit_equal=true",
            "expect:[{left:{a:2},right:{a:3},diff:{values_changed:{\"root['a']\":{right_value:3,left_value:2}}}}]",
        ]
    )


def test_ddiff_omit_equal_bare_flag_same_as_true():
    execute_tokens(
        [
            "[{a:1},{a:2}]",
            "[{a:1},{a:3}]",
            "ddiff@omit_equal",
            "expect:[{left:{a:2},right:{a:3},diff:{values_changed:{\"root['a']\":{right_value:3,left_value:2}}}}]",
        ]
    )


def test_ddiff_ignore_order():
    execute_tokens(
        [
            "[{xs:[1,2,3]}]",
            "[{xs:[3,2,1]}]",
            "ddiff@ignore_order=true",
            "expect:[{left:{xs:[1,2,3]},right:{xs:[3,2,1]},diff:{}}]",
        ]
    )


def test_ddiff_significant_digits_with_omit_equal():
    execute_tokens(
        [
            "[{x:1.001}]",
            "[{x:1.002}]",
            "ddiff@significant_digits=1@omit_equal=true",
            "expect:[]",
        ]
    )
