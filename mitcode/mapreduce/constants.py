class MessageType(object):
    DO_MAP_PHRASE = "DoMapPhrase"
    DO_REDUCE_PHRASE = "DoReducePhrase"
    MAP_DONE = "MapDone"
    REDUCE_DONE = "ReduceDone"
    SUBMIT_JOB = "SubmitJob"
    PHRASE_DONE = "PhraseDone"
    CONTINUE = "ContinueJob"
    TASK_DONE = "TaskDone"
    SCHEDULE_TASK = "ScheduleTask"
    LAUNCH_TASK = "LaunchTask"
    ASSIGN_TASK = "AssignTask"
    JOB_DONE = "JobDone"
    GET_JOB_STATUS = "GetJobStatus"
    REGISTER_WORKER = "RegisterWorker"
    SCHEDULE_MAP_TASK = "ScheduleMapTASK"
    SCHEDULE_REDUCE_TASK = "ScheduleReduceTASK"


class TaskType(object):
    MAP = "Map"
    REDUCE = "Reduce"


class RegisterStatus(object):
    NOT_REGISTERED = "NotRegistered"
    REGISTERED = "Registered"


class TaskStatus(object):
    NOT_STARTED = "NotStarted"
    DONE = "Done"
    FAIL = "Fail"


class JobStatus(object):
    NOT_STARTED = "NotStarted"
    DONE = "Done"
    FAIL = "Fail"
