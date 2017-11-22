import uuid


class Job(object):
    def __init__(self, files, reduce_num):
        self.job_id = uuid.uuid4()
        self.files = files
        self.reduce_num = reduce_num

    def wait(self):
        pass

    def assert_success(self):
        pass

    def _map_func(self):
        pass

    def _reduce_func(self):
        pass
