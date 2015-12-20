from ggprovisioner import SimpleStringifiable


class Job(SimpleStringifiable):
    """
    A class to represent and maintain jobs.
    """
    def __init__(self, tenant_addr, id_num, status, req_time=None,
                 req_cpu=None, req_mem=None, req_disk=None,
                 description=None, fulfilled=False):
        self.tenant_address = tenant_addr
        self.id = id_num
        self.status = status
        self.req_time = req_time
        self.req_cpus = req_cpu
        self.req_mem = req_mem
        self.fulfilled = fulfilled
        self.launch = None

        self.ondemand = False
        self.tool = None
        self.version = None
        if description is not None:
            if "ondemand" in description:
                self.ondemand = description['ondemand']
            if "tool" in description:
                self.tool = description['tool']
            if "version" in description:
                self.version = description['version']
