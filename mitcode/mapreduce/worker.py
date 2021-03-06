import uuid

from mitcode.mapreduce.constants import MessageType


class Worker(object):
    def __init__(self):
        self.worker_id = uuid.uuid4()
        self._worker = self._worker_loop()
        self.master = None
        super().__init__()

    def assign_task(self, task_args):
        self._worker.send((MessageType.ASSIGN_TASK, task_args))

    def finish_task(self, task_id):
        self._worker.send((MessageType.TASK_DONE, task_id))

    def _worker_loop(self):
        tasks = {}
        while True:
            message_type, message = yield
            if message_type == MessageType.ASSIGN_TASK:
                task = message
                task.launch_task()
                tasks[task.task_id] = task
            elif message_type == MessageType.TASK_DONE:
                task_id = message
                tasks[task_id].close_task()

    def start(self):
        next(self._worker)
