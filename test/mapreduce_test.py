import os
import unittest
import uuid

from mitcode.job import Job
from mitcode.worker import Worker

from mitcode.mapreduce.master import Master


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

    def test_basic(self):
        master = Master("name")
        master.start()

        job = Job("job", self._mapFunc(), self.reduceFunc)

        for i in range(2):
            worker = Worker(uuid.uuid4(), master.address)
            worker.doTask(job)

        job.wait()

        self.check()
        self._checkWorkers()

    def tearDown(self):
        pass
