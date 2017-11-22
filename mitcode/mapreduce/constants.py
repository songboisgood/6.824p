class MasterCommand(object):
    Register = "Register"


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
