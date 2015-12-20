from ggprovisioner import SimpleStringifiable


class Instance(SimpleStringifiable):
    """
    A class to manage AWS instance types.
    """
    def __init__(self, db_id, ins_type, ondemand, cpus, memory, disk, ami):
        self.db_id = db_id
        self.type = ins_type
        self.ondemand = ondemand
        self.cpus = cpus
        self.memory = memory
        self.disk = disk
        self.ami = ami
        self.spot = {}
