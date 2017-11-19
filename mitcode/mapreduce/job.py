import uuid


class Job(object):
    def _reduce_name(self, map_task_id, reduce_task_id):
        return "{0}-{1}-{2}.txt".format(self.job_id, map_task_id, reduce_task_id)

    def __init__(self, map_func, reduce_func, reduce_num):
        self.job_id = uuid.uuid4()
        self.map_func = map_func
        self.reduce_func = reduce_func
        self.reduce_num = reduce_num

    def do_map(self, map_task_id, map_file):
        key_value_pairs = self.map_func(map_file)

        for (key, value) in key_value_pairs:
            reduce_task_id = hash(key) % self.reduce_num

            with open(self._reduce_name(map_task_id, reduce_task_id), 'a') as f:
                f.write("{0}\t{1}\n".format(key.encode('string_escape'), value.encode('string_escape')))
