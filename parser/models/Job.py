#
# Assumes that Spark Jobs run in FIFO since it is the default job scheduling policy.
#
class Job(object):

    def __init__(self, id, submissionTime, completionTime, length, dag, numStages, stages):
        super(Job, self).__init__()
        self._id = id
        self._submissionTime = float(submissionTime)
        self._completionTime = float(completionTime)
        self._length = float(length) # in miliseconds
        self._dag = dag
        self._numStages = numStages

        # Stage is a map because it is more efficient when manipulating the DAG
        self._stages = stages

    def getStages(self):
        return self._stages

    def getStage(self, id):
        return self._stages[id]

    def getDag(self):
        return self._dag

    def getNumStages(self):
        return self._numStages

    def getLength(self):
        return self._length

    def __str__(self):
        result = '{"job": ' + str(self._id) + ', "jobTime:" ' + str(self._length) + ', "stages": ['
        first = True
        for s in self._stages:
            if not first:
                result += ', '
            result += str(self._stages[s])
            first = False
        result += ']}'
        return result
