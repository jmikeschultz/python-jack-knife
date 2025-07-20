from djk.base import Source, Sink, ParsedToken, Usage
from djk.sources.inline_source import InlineSource
import sys

class ExpectSink(Sink):
    def __init__(self, input_source: Source, ptok: ParsedToken, usage: Usage):
        super().__init__(input_source)
        self.inline = ptok.whole_token.split(':', 1)[-1] # need raw token cuz : in there
        self.expect_source = InlineSource(self.inline)

    def process(self) -> None:
        command = ' '.join(sys.argv[1:-1]) # omit pjk and expect

        num_put = 0
        while True:
            test_rec = self.input.next()
            if test_rec is None:
                break

            expect_rec = self.expect_source.next()
            if expect_rec is None:
                message = f'expect failure: {command}\nexpected_record:None\ngot_record:{test_rec}\nentire_expected:{self.inline}'
                raise ValueError(message)

            if test_rec != expect_rec:
                message = f'expect failure: {command}\nexpected_record:{expect_rec}\ngot_record:{test_rec}\nentire_expected:{self.inline}'
                raise ValueError(message)
            
        expect_rec = self.expect_source.next() # should be None
        if expect_rec is not None:
                message = f'expect failure: {command}\nexpected_record:{expect_rec}\ngot_record:None\nentire_expected:{self.inline}'
                raise ValueError(message)
        
        print(f'{command} ==> OK!\n')

            

            
    
