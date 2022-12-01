class DataSourceError(KeyError):
    def __init__(self, source: str, target: str):
        self._source = source
        self._target = target

    def __str__(self):
        return f"Source {self._source} reports no such data for Target {self._target}"
