import uuid

from mitcode.mapreduce.constants import MasterCommand, RegisterStatus


class Master(object):
    def __init__(self):
        self.master_id = uuid.uuid4()
        self.r = self.run_master()
        self.scheduler = self.run_scheduler()

    def register(self, worker):
        self.r.send(MasterCommand.Register, worker)

    def run_master(self):
        workers = []
        worker_id = None
        while True:
            command, worker_id = yield RegisterStatus.Registered, worker_id
            if command == MasterCommand.Register:
                workers.append(worker_id)
                self.scheduler.send(worker_id)

    def run_scheduler(self):
        while True:
            work_id = yield 


    def start(self):
        next(self.r)
        next(self.scheduler)

    def do_job(self, job):
        pass

    def wait_job(self, job):
        pass
