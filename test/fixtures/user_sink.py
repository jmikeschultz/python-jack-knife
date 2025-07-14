from djk.base import Sink, Source, ParsedToken

class MySink(Sink):
    def __init__(self, ptok: ParsedToken, input_source: Source):
        super().__init__(input_source)

    def process(self):
        while True:
            row = self.input.next()
            if row is None:
                break
            print(row)
