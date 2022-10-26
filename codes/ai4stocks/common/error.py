class CodeNotFinishError(Exception):
    def __init__(self):
        self.args = (0, 'This part seems still to be under construction.')