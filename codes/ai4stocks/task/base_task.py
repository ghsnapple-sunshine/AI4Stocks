class BaseTask:
    def __init__(self,
                 obj: object,
                 method_name: str,
                 args: tuple = None,
                 kwargs: dict = None):
        self.obj = obj
        self.method_name = method_name
        self.args = args
        self.kwargs = kwargs

    def run(self) -> bool:
        if not hasattr(self.obj, self.method_name):
            return False
        valid_args = isinstance(self.args, tuple)
        valid_kwargs = isinstance(self.kwargs, dict)
        if valid_args & valid_kwargs:
            return getattr(self.obj, self.method_name)(*self.args, **self.kwargs)
        elif valid_args:
            return getattr(self.obj, self.method_name)(*self.args)
        elif valid_kwargs:
            return getattr(self.obj, self.method_name)(**self.kwargs)
        else:
            return getattr(self.obj, self.method_name)()
