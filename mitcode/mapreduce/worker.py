import uuid

from mitcode.mapreduce.constants import TaskStatus, TaskType
from mitcode.mapreduce.task import Task


class Worker(object):
    def __init__(self):
        self.worker_id = uuid.uuid4()
        self.r = self.run_worker()
        self.master = None
        super().__init__()

    def do_task(self, job_id, task_id, task_args):

        self.tasks.append(task)
        task.run_task(task_args)

    def run_worker(self):
        tasks = []
        while True:
            task_args = yield (status, job_id, task_id)
            task = Task(job_id, task_id, self)
            task.do_task()
            if task_args.type_type == TaskType.Map:
                self._do_map(task_args)
            elif task_args.type_type == TaskType.Reduce:
                self._do_reduce(task_args)

            status = TaskStatus.Done

    def _do_map(self, task_args):
        pass

    def _do_reduce(self, task_args):
        pass

    def start(self):
        next(self.r)
