import uuid

from mitcode.mapreduce.constants import MessageType, TaskStatus


class Task(object):
    def __init__(self, job_id, func):
        self.job_id = job_id
        self.func = func
        self.task_id = uuid.uuid4()
        self.t = self.task()

    def task(self):
        while True:
            message_type = yield
            if message_type == MessageType.LaunchTask:
                self.do_task()
                self.worker.send_message(TaskStatus.Done)

    def do_task(self):
        pass

    def _reduce_name(self, reduce_task_id):
        return "{0}-{1}-{2}.txt".format(self.job_id, self.task_id, reduce_task_id)

    def launch_task(self):
        next(self.t)
        self.t.send(MessageType.LaunchTask)

    def close_task(self):
        self.t.close()
