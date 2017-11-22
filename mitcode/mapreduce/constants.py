class MessageType(object):
    PHRASE_DONE = "PhraseDone"
    CONTINUE_JOB = "ContinueJob"
    TASK_DONE = "TaskDone"
    SCHEDULE_TASK = "ScheduleTask"
    LAUNCH_TASK = "LaunchTask"
    ASSIGN_TASK = "AssignTask"
    JOB_DONE = "JobDone"
    GET_JOB_STATUS = "GetJobStatus"
    REGISTER_WORKER = "RegisterWorker"
    SCHEDULE_JOB = "ScheduleJob"


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
