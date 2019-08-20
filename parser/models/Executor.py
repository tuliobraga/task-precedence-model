import sys

class Executor(object):

    def __init__(self, id, cores):
        super(Executor, self).__init__()
        self._id = id
        self._cores = cores

    def __str__(self):
        return str("Exec ID: " + str(self._id) + ", Cores: " + str(self._cores))
