from djk.base import Source, Pipe, SyntaxError

def add_operator(op, stack):
    # checking methods avoids circular deps
    # LEADS TO PROBLEMNS IS METHOD NAMES CHANGE
    if len(stack) > 0 and isinstance(stack[-1], Pipe):
        target = stack[-1]
        add_subop = getattr(target, "add_subop", None)
        if callable(add_subop): # means target = subexp = [
            get_over = getattr(op, 'get_over_arg', None)
            if callable(get_over): # means op = subexp_over
                subexp_begin = stack.pop()
                subexp_begin.set_over_arg(get_over())
                op.set_sources([subexp_begin])
                stack.append(op)
                return
            else: # an operator within the subexpression
                add_subop(op)
                return

    # order matters, sources are pipes
    if isinstance(op, Pipe):
        arity = op.arity # class level attribute
        if len(stack) < arity:
            raise SyntaxError(f"'{op}' requires {arity} input(s)")
        op.set_sources([stack.pop() for _ in range(arity)][::-1])
        stack.append(op)
        return

    elif isinstance(op, Source):
        stack.append(op)
        return

class SafeNamespace:
    def __init__(self, obj):
        for k, v in obj.items():
            if isinstance(v, dict):
                v = SafeNamespace(v)
            elif isinstance(v, list):
                v = [SafeNamespace(x) if isinstance(x, dict) else x for x in v]
            setattr(self, k, v)

    def __getattr__(self, key):
        return None  # gracefully handle missing keys
