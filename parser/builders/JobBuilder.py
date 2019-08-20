from ..models.Job import Job

# JobBuilder is a mutable object and should only be used to create a immutable Job object!"
class JobBuilder(object):

    def __init__(self):
        super(JobBuilder, self).__init__()
        self._stages = {}
        self._submissionTime = 0
        self._completionTime = 0

    def start(self, id, submissionTime):
        self._id = str(id)
        self._submissionTime += submissionTime
        self._dag = {}

    def finish(self, completionTime):
        self._completionTime += completionTime

    def addStage(self, stageId, stage):
        self._stages[stageId] = stage

    def updateDag(self, stageId, parentIds):
        if not self._dag.has_key(stageId):
            self._dag[stageId] = set()

        for pid in parentIds:
            # need to test if stage exists before adding it to the dag
            # some stages may have been executed in previous jobs
            # then, its result is reused without reexecution
            if self._stages.has_key(pid):
                self._dag[stageId].add(pid)

    # def updateDag(self, stageId, parentIds):
    #     if not parentIds:
    #         self._dag['root'].append(stageId)
    #     else:
    #         for id in parentIds:
    #             if not self._dag.has_key(id):
    #                 self._dag[id] = []
    #             if not stageId in self._dag[id]:
    #                 self._dag[id].append(stageId)

    # more efficient by using topological search
    def computeOverlapMatrix():
        added = []
        visited = []


    def build(self, counter):
        self._submissionTime /= counter
        self._completionTime /= counter
        self._computeJobLength()

        # Building Stage immutable objects
        # Keeping stage as a map because it is more efficient when working with
        # the stages from the dag of stages
        stages = {}
        self._numStages = 0
        for sid in self._stages:
            self._numStages += 1
            stage = self._stages[sid]
            stages[sid] = stage.build(counter)

        return Job(self._id, self._submissionTime, self._completionTime, self._length, self._dag, self._numStages, stages)

    def _computeJobLength(self):
        self._length = self._completionTime - self._submissionTime

    def __str__(self):
        return str("JOB_" + self._id + ":" + self._submissionTime)


# def setCompletionTime(self, time):
#     self._completionTime = time
#     self._computeJobLength()
#
