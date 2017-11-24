from mitcode.mapreduce.task import Task


class ReduceTask(Task):

    def __init__(self, job_id, reduce_func, key, files):
        self.files = files
        self.key = key

        super().__init__(job_id, reduce_func)
