from mitcode.mapreduce.task import Task


class MapTask(Task):
    def __init__(self, job_id, map_func, file, reduce_num):
        self.file = file
        self.reduce_num = reduce_num

        super().__init__(job_id, map_func)

    def do_map(self):
        key_value_pairs = self.func(self.file)

        for (key, value) in key_value_pairs:
            reduce_task_id = hash(key) % self.reduce_num

            with open(self._reduce_name(reduce_task_id), 'a') as f:
                f.write("{0}\t{1}\n".format(key.encode('string_escape'), value.encode('string_escape')))

    def do_task(self):
        self.do_map()



