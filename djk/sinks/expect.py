from djk.base import Source, Sink, ParsedToken, Usage
from djk.sources.inline_source import InlineSource
import sys

class ExpectSink(Sink):
    def __init__(self, input_source: Source, ptok: ParsedToken, usage: Usage):
        super().__init__(input_source)
        inline = ptok.whole_token.split(':', 1)[-1] # need raw token cuz : in there
        self.expect_source = InlineSource(inline)

    def process(self) -> None:
        command = ' '.join(sys.argv[1:])
        num_put = 0
        while True:
            test_rec = self.input.next()
            if test_rec is None:
                break

            expect_rec = self.expect_source.next()
            if expect_rec is None:
                message = f'expect failure\nexpected:None\ngot:{test_rec}'
                raise ValueError(message)

            if test_rec != expect_rec:
                message = f'expect failure\nexpected:{expect_rec}\ngot:{test_rec}'
                raise ValueError(message)
            
        print(f'{command} OK!\n')

            

            
    
