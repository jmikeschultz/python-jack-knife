from djk.base import Source

class MySource(Source):
    def __init__(self, arg_string=""):
        self.done = False
        self.max_copies = 7
        self.num_copied = 0
        self.instance_no = int(arg_string) if arg_string else 0

    def next(self):
        if not self.done:
            self.done = True
            return {"instance_no": self.instance_no}
        return None

    def deep_copy(self):
        if self.num_copied >= self.max_copies:
            return None

        self.num_copied += 1
        return MySource(str(self.num_copied))
