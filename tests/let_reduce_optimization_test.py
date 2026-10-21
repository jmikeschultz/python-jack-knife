from pjk.pipes.let_reduce import LetPipe, ReducePipe
from pjk.usage import ParsedToken


def make_pipe(pipe_class, token_text, records):
    token = ParsedToken(token_text)
    usage = pipe_class.usage()
    usage.bind(token)
    pipe = pipe_class(token, usage)
    pipe.add_source(records)
    return pipe


class UnreadableList(list):
    def __iter__(self):
        raise AssertionError("expression eagerly iterated an unused list")

    def __getitem__(self, index):
        raise AssertionError("expression eagerly read an unused list element")


def test_let_len_field_does_not_read_list_elements():
    pipe = make_pipe(
        LetPipe,
        "let:count=len(f.child)",
        [{"child": UnreadableList([{"value": 1}, {"value": 2}])}],
    )

    assert list(pipe)[0]["count"] == 2


def test_let_does_not_read_unreferenced_nested_list():
    pipe = make_pipe(
        LetPipe,
        "let:value=f.details.value",
        [{"details": {"value": 7}, "child": UnreadableList([{"unused": True}])}],
    )

    assert list(pipe)[0]["value"] == 7


def test_reduce_constant_does_not_read_record_fields():
    pipe = make_pipe(
        ReducePipe,
        "reduce:count+=1",
        [{"child": UnreadableList([{"unused": True}])} for _ in range(3)],
    )

    list(pipe)

    assert pipe.get_subexp_result() == ("count", 3)


def test_reduce_reads_only_referenced_field():
    pipe = make_pipe(
        ReducePipe,
        "reduce:total+=f.value",
        [
            {"value": 2, "child": UnreadableList([{"unused": True}])},
            {"value": 3, "child": UnreadableList([{"unused": True}])},
        ],
    )

    list(pipe)

    assert pipe.get_subexp_result() == ("total", 5)


def test_let_materializes_a_returned_list_field():
    child = [{"value": 1}, {"value": 2}]
    pipe = make_pipe(LetPipe, "let:copy=f.child", [{"child": child}])

    result = list(pipe)[0]

    assert result["copy"] == child
    assert isinstance(result["copy"], list)


def test_reduce_materializes_nested_list_results():
    pipe = make_pipe(
        ReducePipe,
        "reduce:groups+=[f.i]",
        [{"i": [1, 2]}, {"i": [3]}],
    )

    list(pipe)

    assert pipe.get_subexp_result() == ("groups", [[1, 2], [3]])
