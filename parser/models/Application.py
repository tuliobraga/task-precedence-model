#
# Assumes that Spark Jobs run in FIFO since it is the default job scheduling policy.
#
class Application(object):

    def __init__(self, id, startTime, endTime, appLength, execLength, numStages, jobs):
        super(Application, self).__init__()
        self._id = id
        self._startTime = float(startTime)
        self._endTime = float(endTime)
        self._jobs = jobs
        self._numStages = numStages

        # in miliseconds - time between Application Start and Application End
        self._applicationLength = float(appLength)
        # in miliseconds - time between first stage submission and last stage completion
        self._executionLength = float(execLength) # in miliseconds

    def getJobs(self):
        return self._jobs

    def getExecutionLength(self):
        return self._executionLength

    def getNumStages(self):
        return self._numStages

    def __str__(self):
        result = '{"app:"'+str(self._id) + ', "appTime":'+str(self._applicationLength) + ', "execTime":' + str(self._executionLength) + ', "jobs": ['
        first = True
        for job in self._jobs:
            if not first:
                result += ', '
            result += str(job)
            first = False
        result += ']}'
        return result
