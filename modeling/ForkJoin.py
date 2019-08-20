#
# Assumes that Spark Jobs run in FIFO since it is the default job scheduling policy.
#
class ForkJoin(object):

    def __init__(self, app, cluster):
        super(ForkJoin, self).__init__()
        self._app = app
        self._cluster = cluster

    # def intrapolation():
    #     for job in app.getJobs():
    #         job.getDag
