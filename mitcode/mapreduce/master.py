import asyncio
import uuid

from mitcode.mapreduce.constants import RegisterStatus, MessageType, JobStatus
from mitcode.mapreduce.map_task import MapTask


class Master(object):
    def __init__(self):
        self.master_id = uuid.uuid4()
        self.m = self.master()
        self.s = self.scheduler()

    def register(self, worker):
        self.m.send(MessageType.REGISTER_WORKER, worker)

    def master(self):
        workers = []
        worker_id = None
        while True:
            message_type, worker_id = yield RegisterStatus.REGISTERED, worker_id
            if message_type == MessageType.REGISTER_WORKER:
                workers.append(worker_id)
                self.s.send(MessageType.REGISTER_WORKER, worker_id)

            if message_type == MessageType.PHRASE_DONE:
                pass


    def scheduler(self):
        workers_available = []
        task_queue = []
        task_done = 0
        task_number = 0
        while True:
            (message_type, message) = yield
            if message_type == MessageType.REGISTER_WORKER:
                worker_id = message
                workers_available.append(worker_id)
                self.s.send(MessageType.CONTINUE_JOB)
            elif message_type == MessageType.SCHEDULE_JOB:
                job = message
                task_number = len(job.files)
                for file in job.files:
                    task = MapTask(job.job_id, job.map_func, file, job.reduce_num)
                    if len(workers_available) == 0:
                        task_queue.append(task)
                    else:
                        worker = workers_available.pop(0)
                        worker.assign_task(task)
            elif message_type == MessageType.CONTINUE_JOB:
                for worker in workers_available:
                    if len(task_queue) > 0:
                        task = task_queue.pop(0)
                        worker.assign_task(task)
            elif message_type == MessageType.TASK_DONE:
                worker = message
                workers_available.append(worker.worker_id)
                self.s.send(MessageType.CONTINUE_JOB)
                task_done += 1
                if task_done == task_number:
                    self.m.send(MessageType.PHRASE_DONE)

    def start(self):
        next(self.m)
        next(self.s)

    def submit_job(self, job):
        self._schedule(job)

    def job_waiter(self):
        while True:
            status = yield
            if status == JobStatus.DONE:
                break

    def wait_job(self, job):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.job_waiter())
        loop.close()

    def _schedule(self, job):
        self.s.send(job)

        while True:
            (job_id, status) = self.s.send(job.job_id, MessageType.GET_JOB_STATUS)
            if status == JobStatus.DONE:
                self.m.send(MessageType.JOB_DONE, job.job_id)
