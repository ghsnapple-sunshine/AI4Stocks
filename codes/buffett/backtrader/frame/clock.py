class Clock:
    def __init__(self):
        self._tick = -1
        self._time = 'Prepare'
        self._is_end = False

    @property
    def tick(self):
        return self._tick

    @property
    def time(self):
        return self._time

    @property
    def is_end(self):
        return self._is_end

    def turn_next(self, time, is_end: bool = False) -> None:
        self._tick += 1
        self._time = time
        self._is_end = is_end
