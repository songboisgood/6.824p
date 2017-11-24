from mitcode.mapreduce.constants import MessageType
from mitcode.mapreduce.map_task import MapTask
from mitcode.mapreduce.reduce_task import ReduceTask


class MapPhrase(object):

    def __init__(self, job_id, map_func, files, reduce_num, schedule):
        self.job_id = job_id
        self.map_func = map_func
        self.files = files
        self.reduce_num = reduce_num
        self.schedule = schedule

    def _phrase_loop(self):
        while True:
            message_type, message = yield
            if message_type == MessageType.DO_MAP_PHRASE:
                map_phrase= message
                for file in map_phrase.files:
                    map_task = MapTask(map_phrase.job_id, map_phrase.map_func, file, map_phrase.reduce_num)
                    self.schedule.schedule_task(map_task)

            elif message_type == MessageType.DO_REDUCE_PHRASE:
                reduce_phrase = message
                for key, files in reduce_phrase.files_map:
                    reduce_task = ReduceTask(reduce_phrase.job_id, reduce_phrase.reduce_func, key, files)
                    self.schedule.schedule_task(reduce_task)