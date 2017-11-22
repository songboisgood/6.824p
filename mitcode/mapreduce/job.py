import uuid


class Job(object):

    def __init__(self, files, map_func, reduce_func, reduce_num):
        self.job_id = uuid.uuid4()
        self.files = files
        self.map_func = map_func
        self.reduce_func = reduce_func
        self.reduce_num = reduce_num


    def apply(self, master):
        pass

    def wait(self):
        pass

    def assert_success(self):
        pass
