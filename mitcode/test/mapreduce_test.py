import os
import unittest

from mitcode.mapreduce.job import Job
from mitcode.mapreduce.master import Master
from mitcode.mapreduce.worker import Worker


class MapReduceTest(unittest.TestCase):
    NUM_OF_FILES = 100000
    NUM_OF_MAP = 100
    NUM_OF_REDUCE = 50

    def _mapFunc(self):
        pass

    def _reduceFunc(self):
        pass

    def _makeInput(self, numOfFiles):
        files = []
        for i in range(numOfFiles):
            file = '824-mrinput-{0}.txt'.format(i)
            files.append(file)
            with open(file, 'w') as f:
                for j in range(numOfFiles / self.NUM_OF_FILES):
                    f.write('{0}{1}'.format(j, os.linesep))

        return files

    def setUp(self):
        self.files = self._makeInput(self.NUM_OF_FILES)
        self.master = Master()
        self.master.start()
        for i in range(4):
            worker = Worker()
            worker.start()
            self.master.register(worker)

    def test_basic(self):
        job = Job("job", self.files, self._mapFunc(), self._reduceFunc)
        self.master.do_job(job)
        self.master.wait_job(job)
        job.assert_success()

    def tearDown(self):
        pass
