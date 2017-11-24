import uuid

from mitcode.mapreduce.constants import MessageType
from mitcode.mapreduce.map_task import MapTask
from mitcode.mapreduce.reduce_task import ReduceTask


class Job(object):
    def __init__(self, files, reduce_num):
        self.job_id = uuid.uuid4()
        self.files = files
        self.reduce_num = reduce_num
        self._job_waiter = self.wait()

    def finish_job(self):
        self._job_waiter.send(MessageType.JOB_DONE)

    def assert_success(self):
        pass

    def start(self):
        next(self._job_waiter)

    def wait(self):
        while True:
            message_type, message = yield
            if message_type == MessageType.JOB_DONE and message == self.job_id:
                break

    def _map_func(self):
        pass

    def _reduce_func(self):
        pass


