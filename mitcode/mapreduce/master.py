import uuid

from mitcode.mapreduce.constants import MessageType
from mitcode.mapreduce.map_phrase import MapPhrase
from mitcode.mapreduce.reduce_phrase import ReducePhrase
from mitcode.mapreduce.schedule import Schedule


class Master(object):
    def __init__(self):
        self.master_id = uuid.uuid4()
        self._master = self._master_loop()
        self._schedule = Schedule()

    def register(self, worker):
        self._master.send(MessageType.REGISTER_WORKER, worker)

    def start(self):
        next(self._master)

    def submit_job(self, job):
        self._master.send(MessageType.SUBMIT_JOB, job)

    def _master_loop(self):
        workers = []
        jobs = {}
        self._schedule.start()
        while True:
            message_type, message = yield
            if message_type == MessageType.REGISTER_WORKER:
                worker_id = message
                workers.append(worker_id)
                self._schedule.register_worker(worker_id)

            elif message_type == MessageType.SUBMIT_JOB:
                job = message
                jobs[job.job_id] = job
                job.start()
                map_phrase = MapPhrase(job.job_id, job.map_func, job.files, job.reduce_num)
                self._schedule.schedule_map(map_phrase)

            elif message_type == MessageType.MAP_DONE:
                job_id = message
                job = jobs[job_id]
                reduce_phrase = ReducePhrase(job_id, job.reduce_func)
                self._schedule.schedule_reduce(reduce_phrase)

            elif message_type == MessageType.REDUCE_DONE:
                job_id = message
                jobs[job_id].finish_job()
                jobs.pop(job_id)
