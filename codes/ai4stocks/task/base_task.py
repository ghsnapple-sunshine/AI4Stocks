from pendulum import DateTime


class BaseTask:
    def __init__(
            self,
            obj: object,
            method_name: str,
            args: tuple = None,
            kwargs: dict = None,
            plan_time: DateTime = None
    ):
        self.obj = obj
        self.method_name = method_name
        self.args = args
        self.kwargs = kwargs
        if isinstance(plan_time, DateTime):
            self.plan_time = plan_time
        else:
            self.plan_time = DateTime.now()

    def run(self) -> tuple:
        if not hasattr(self.obj, self.method_name):
            return False, None, None

        tp = type(self.obj)
        print("---------------Start running method {0} in Type {1} object.---------------".format(
            self.method_name, tp
        ))
        valid_args = isinstance(self.args, tuple)
        valid_kwargs = isinstance(self.kwargs, dict)

        if valid_args & valid_kwargs:
            res = getattr(self.obj, self.method_name)(*self.args, **self.kwargs)
        elif valid_args:
            res = getattr(self.obj, self.method_name)(*self.args)
        elif valid_kwargs:
            res = getattr(self.obj, self.method_name)(**self.kwargs)
        else:
            res = getattr(self.obj, self.method_name)()
        print("---------------End running method {0} in Type {1} object.---------------".format(
            self.method_name, tp
        ))
        return True, res, None

    def get_plan_time(self) -> DateTime:
        return self.plan_time
