from ..models.Application import Application
from JobBuilder import JobBuilder
from StageBuilder import StageBuilder
from TaskBuilder import TaskBuilder

# ApplicationBuilder is a mutable object and should only be used to create a mutable Application object!"
# This builder combines one or more log files together to get an averaged Application object.
class ApplicationBuilder(object):

    def __init__(self):
        super(ApplicationBuilder, self).__init__()
        self._jobs = {}
        self._id = ""
        self._startTime = 0
        self._endTime = 0
        self._counter = 0
        self._firstStageStartTime = 0
        self._lastStageEndTime = 0

    def start(self, id, startTime):
        self._id += ":" + id
        self._counter += 1
        self._startTime += startTime
        self._isFirstStage = True
        self._firstStageStartTimeAux = -1
        self._stageToJob = {}
        self._jobKeys = {}
        self._jobKeyId = {}

    def finish(self, time):
        self._endTime += time
        self._firstStageStartTime += self._firstStageStartTimeAux
        self._lastStageEndTime += self._lastStageEndTimeAux

    def startJob(self, jobId, jobIdentifier, submissionTime, stageIds):
        # since the job always start prior to the stages and its tasks,
        # when fetching the job id to start/finish a stage or task
        # the job id value will always be updated to the job id
        # of the current Spark event log

        # Update Job Identifier ID - prevent equal stage jobs to be merged
        # if self._jobKeyId.has_key(jobIdentifier) == False:
        #     self._jobKeyId[jobIdentifier] = 0
        # else:
        #     self._jobKeyId[jobIdentifier] += 1
        #
        # jobIdentifier = jobIdentifier + ":" + str(self._jobKeyId[jobIdentifier])

        self.updateStageToJobMap(jobIdentifier, stageIds)
        self._jobKeys[jobId] = jobIdentifier

        # fetch or create JobBuilder
        if self._jobs.has_key(jobIdentifier):
            job = self._jobs[jobIdentifier]
        else:
            job = JobBuilder()
            self._jobs[jobIdentifier] = job

        job.start(jobIdentifier, submissionTime)

    def finishJob(self, jobId, completionTime):
        jobKey = self._jobKeys[jobId]
        job = self._jobs[jobKey]
        job.finish(completionTime)

    def startStage(self, stageId, parentIds, stageName, numberOfTasks):
        # since the job always start prior to the stages and its tasks,
        # when fetching the job id to start/finish a stage or task
        # the job id value will always be updated to the job id
        # of the current Spark event log
        identifier = str(stageName) + ":" + str(numberOfTasks)
        jobKey = self._stageToJob[stageId]
        job = self._jobs[jobKey]
        job.updateDag(stageId, parentIds)
        stages = job._stages

        # fetch or create StageBuilder
        if stages.has_key(stageId):
            stage = stages[stageId]
        else:
            stage = StageBuilder()
            self._jobs[jobKey].addStage(stageId, stage)

        stage.start(stageId)

    def finishStage(self, stageId, submissionTime, completionTime):
        # since the job always start prior to the stages and its tasks,
        # when fetching the job id to start/finish a stage or task
        # the job id value will always be updated to the job id
        # of the current Spark event log
        jobKey = self._stageToJob[stageId]
        stage = self._jobs[jobKey]._stages[stageId]

        # defining the first stage submission time
        if submissionTime < self._firstStageStartTimeAux or self._firstStageStartTimeAux == -1:
            self._firstStageStartTimeAux = submissionTime

        stage.finish(submissionTime, completionTime)
        self._lastStageEndTimeAux = completionTime

    def startTask(self, stageId, taskId, startTime, executorId):
        # since the job always start prior to the stages and its tasks,
        # when fetching the job id to start/finish a stage or task
        # the job id value will always be updated to the job id
        # of the current Spark event log
        jobKey = self._stageToJob[stageId]
        stage = self._jobs[jobKey]._stages[stageId]
        tasks = stage._tasks

        # fetch or create StageBuilder
        if tasks.has_key(taskId):
            task = tasks[taskId]
        else:
            task = TaskBuilder(executorId)
            stage.addTask(taskId, task);

        stage.startTask(taskId, startTime)

    def finishTask(self, stageId, taskId, endTime):
        # since the job always start prior to the stages and its tasks,
        # when fetching the job id to start/finish a stage or task
        # the job id value will always be updated to the job id
        # of the current Spark event log
        jobKey = self._stageToJob[stageId]
        stage = self._jobs[jobKey]._stages[stageId]
        stage.finishTask(taskId, endTime)

    def updateStageToJobMap(self, jobKey, stageIds):
        for sid in stageIds:
            self._stageToJob[sid] = jobKey

    def build(self):
        self._startTime /= self._counter
        self._endTime /= self._counter
        self._firstStageStartTime /= self._counter
        self._lastStageEndTime /= self._counter
        self._computeApplicationLength()
        self._computeExecutionLength()

        # Building job immuatable objects
        jobs = []
        numStages = 0
        for jid in self._jobs:
            job = self._jobs[jid]
            built = job.build(self._counter)
            # print built._stages
            numStages += built.getNumStages()
            jobs.append(built)

        print numStages

        return Application(self._id, self._startTime, self._endTime, self._applicationLength, self._executionLength, numStages, jobs)

    def _computeApplicationLength(self):
        self._applicationLength = self._endTime - self._startTime

    def _computeExecutionLength(self):
        self._executionLength = self._lastStageEndTime - self._firstStageStartTime

    def __str__(self):
        return str(self._id + ":" + self._startTime)
