import sys

class Cluster(object):

    def __init__(self, totalCores, executors):
        super(Cluster, self).__init__()
        self._totalCores = totalCores
        self._executors = executors

    def getTotalCores(self):
        return self._totalCores

    def __str__(self):
        return str("Cluster with " + str(self._totalCores) + " cores")
