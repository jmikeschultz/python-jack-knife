from djk.parser import DjkExpressionParser
from djk.base import Usage, ParsedToken

def test_parser():
    parser = DjkExpressionParser(["[{id: 1}, {id: 2}]", "-"])
    sink = parser.parse()
    assert sink.input.next() == {'id': 1}
    assert sink.input.next() == {'id': 2}

    parser = DjkExpressionParser(["[{id: 1, cars:[{size:4, color:'blue'}, {size:10, color:'green'}]}]",
                              "[", "let:^tot_car_size+=f.size", "over:cars", "-"])
    sink = parser.parse()
    rec = sink.input.next()
    assert rec.get('tot_car_size') == 14
