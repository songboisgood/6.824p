from mitcode.mapreduce.constants import MessageType
from mitcode.mapreduce.map_task import MapTask
from mitcode.mapreduce.reduce_task import ReduceTask


class Schedule(object):
    def __init__(self, master):
        self._scheduler = self._scheduler_loop()
        self._master = master

    def register_worker(self, worker_id):
        self._scheduler.send(MessageType.REGISTER_WORKER, worker_id)

    def _scheduler_loop(self):
        workers_available = []
        task_queue = []
        task_done = 0
        task_number = 0
        while True:
            (message_type, message) = yield
            if message_type == MessageType.REGISTER_WORKER:
                worker_id = message
                workers_available.append(worker_id)
                self._scheduler.send(MessageType.CONTINUE)
            elif message_type == MessageType.SCHEDULE_TASK:
                task = message
                if len(workers_available) == 0:
                    task_queue.append(task)
                else:
                    worker = workers_available.pop(0)
                    worker.assign_task(task)
            elif message_type == MessageType.CONTINUE:
                if len(task_queue) and len(workers_available) > 0:
                    task = task_queue.pop(0)
                    worker = workers_available.pop(0)
                    worker.assign_task(task)

                if len(task_queue) and len(workers_available) > 0:
                    self._scheduler.send(MessageType.CONTINUE)

            elif message_type == MessageType.TASK_DONE:
                worker_id = message
                self.register_worker(worker_id)
                task_done += 1
                if task_done == task_number:
                    self._master.finishPhrase()

    def start(self):
        next(self._scheduler)

    def schedule_map(self, map_phrase):
        for file in map_phrase.files:
            map_task = MapTask(map_phrase.job_id, map_phrase.map_func, file, map_phrase.reduce_num)
            self._scheduler.send(MessageType.SCHEDULE_TASK, map_task)

    def schedule_reduce(self, reduce_phrase):
        for key, files in reduce_phrase.files_map:
            reduce_task = ReduceTask(reduce_phrase.job_id, reduce_phrase.reduce_func, key, files)
            self._scheduler.send(MessageType.SCHEDULE_TASK, reduce_task)

    def schedule_task(self, task):
        self._scheduler.send(MessageType.SCHEDULE_TASK, task)
