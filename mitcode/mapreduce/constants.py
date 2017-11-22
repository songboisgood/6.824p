class MessageType(object):
    ContinueJob = "ContinueJob"
    TaskDone = "TaskDone"
    ScheduleTask = "ScheduleTask"
    LaunchTask = "LaunchTask"
    AssignTask = "AssignTask"
    JobDone = "JobDone"
    GetJobStatus = "GetJobStatus"
    RegisterWorker = "RegisterWorker"
    ScheduleJob = "ScheduleJob"


class TaskType(object):
    Map = "Map"
    Reduce = "Reduce"


class RegisterStatus(object):
    NotRegistered = "NotRegistered"
    Registered = "Registered"


class TaskStatus(object):
    NotStarted = "NotStarted"
    Done = "Done"
    Fail = "Fail"


class JobStatus(object):
    NotStarted = "NotStarted"
    Done = "Done"
    Fail = "Fail"
