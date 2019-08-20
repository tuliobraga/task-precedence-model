from ..models.Executor import Executor
from ..models.Cluster import Cluster

# TaskBuilder is a mutable object and should only be used to create a immutable Task object!"
class ClusterBuilder(object):

    def __init__(self):
        super(ClusterBuilder, self).__init__()
        self._totalCores = 0
        self._executors = {}

    def addExecutor(self, id, cores):
        exct = Executor(id, cores)
        self._totalCores += cores
        self._executors[id] = exct

    def build(self):
        return Cluster(self._totalCores, self._executors)

    def getTotalCores(self):
        return self._totalCores

    def __str__(self):
        return str("Cluster with " + str(self._totalCores) + " cores")

# def setEndTime(self, time):
#     self._endTime = time
#     self._computeTaskLength()
