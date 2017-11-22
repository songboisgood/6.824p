import asyncio
import queue
import uuid

from mitcode.mapreduce.constants import RegisterStatus, TaskType, MessageType, JobStatus
from mitcode.mapreduce.maptask import MapTask


class Master(object):
    def __init__(self):
        self.master_id = uuid.uuid4()
        self.m = self.master()
        self.s = self.scheduler()

    def register(self, worker):
        self.m.send(MessageType.RegisterWorker, worker)

    def master(self):
        workers = []
        worker_id = None
        while True:
            command, worker_id = yield RegisterStatus.Registered, worker_id
            if command == MessageType.RegisterWorker:
                workers.append(worker_id)
                self.s.send(command, worker_id)

    def scheduler(self):
        workers_available = []
        task_queue = []
        while True:
            (message_type, message) = yield
            if message_type == MessageType.RegisterWorker:
                worker_id = message
                workers_available.append(worker_id)
                self.m.send(MessageType.ContinueJob)
            elif message_type == MessageType.ScheduleJob:
                job = message
                for file in job.files:
                    task = MapTask(job.job_id, job.map_func, file, job.reduce_num)
                    if len(workers_available) == 0:
                        task_queue.append(task)
                    else:
                        worker = workers_available.pop(0)
                        worker.assign_task(task)
            elif message_type == MessageType.ContinueJob:
                for worker in workers_available:
                    if len(task_queue) > 0:
                        task = task_queue.pop(0)
                        worker.assign_task(task)



    def start(self):
        next(self.m)
        next(self.s)

    def do_job(self, job):
        self._schedule(job, TaskType.Map)
        self._schedule(job, TaskType.Reduce)

    def job_waiter(self):
        while True:
            status = yield
            if status == JobStatus.Done:
                break

    def wait_job(self, job):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.job_waiter())
        loop.close()

    def _schedule(self, job, task_type):
        self.s.send(job, task_type)

        while True:
            (job_id, status) = self.s.send(job.job_id, MessageType.GetJobStatus)
            if status == JobStatus.Done:
                self.m.send(MessageType.JobDone, job.job_id)
