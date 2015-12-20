import psycopg2
import datetime
import calendar
import time

from ggprovisioner import logger, ProvisionerConfig, tenant, scheduler
from ggprovisioner.cloud import aws
from ggprovisioner.scheduler.condor.condor_scheduler import CondorScheduler


class Provisioner(object):
    """
    A provisioner for cloud resources.
    Cost effectively acquires and manages instances.
    """
    def __init__(self):
        self.tenants = []

        # Read in any config data and set up the database connection
        ProvisionerConfig()

    def run(self):
        """
        Run the provisioner. This should execute periodically and
        determine what actions need to be taken.
        """

        while True:
            # Get the tenants from the database and process the current
            # condor_q. Also assign those jobs to each tenant.
            self.load_tenants_and_jobs()

            # provisioning will fail if there are no tenants
            if len(self.tenants) > 0:
                # Handle all of the existing requests. This will cancel or
                # migrate excess requests and update the database to reflect
                # the state of the environment
                self.manage_resources()

                # Work out the price for each instance type and acquire
                # resources for jobs
                self.provision_resources()

            # wait "run_rate" seconds before trying again
            time.sleep(ProvisionerConfig().run_rate)

    def load_tenants_and_jobs(self):
        """
        Get all of the tenants from the database and then read the condor
        queue to get their respective jobs.
        """
        # Load all of the tenants
        self.tenants = tenant.load_from_db()

        # Load all of the jobs from condor and associate them with the tenants.
        # This will also remove jobs that should not be processed (e.g. an
        # instance has been fulfilled for them already).
        sched = CondorScheduler()
        sched.load_jobs(self.tenants)

        # Print out what we found
        logger.debug("Found the following tenants:")
        for t in self.tenants:
            logger.debug(repr(t))

    def manage_resources(self):
        """
        Use the resource manager to keep the database up to date and manage
        aws requests and resources.
        """
        # Build a set of instances and their current spot prices so we don't
        # need to keep revisiting the AWS API
        ProvisionerConfig().load_instance_types()

        aws.manager.process_resources(self.tenants)

        scheduler.base_scheduler.ignore_fulfilled_jobs(self.tenants)

    def provision_resources(self):
        # This passes tenant[0] (a test tenant with my credentials) to use its
        # credentials to query the AWS API for price data
        # price data is stored in the Instance objects
        aws.api.get_spot_prices(ProvisionerConfig().instance_types,
                                self.tenants[0])

        # Select a request to make for each job
        self.select_instance_type(ProvisionerConfig().instance_types)
        # Make the requests for the resources
        for t in self.tenants:
            aws.api.request_resources(t)

    def get_potential_instances(self, eligible_instances, job):
        """
        Make a list of all <type,zone> and <type,ondemand> pairs then order
        them.
        """
        unsorted_instances = []
        # Add an entry for each instance type as ondemand, or each spot price
        # so we can sort everything and pick the cheapest.
        for ins in eligible_instances:
            unsorted_instances.append(aws.Request(
                ins, ins.type, "", ins.ami, 1, 0, True,
                ins.ondemand, ins.ondemand))
            # Don't bother adding spot prices if it is an ondemand request:
            if not job.ondemand:
                for zone, price in ins.spot.iteritems():
                    unsorted_instances.append(aws.Request(
                        ins, ins.type, zone, ins.ami, 1, 0, False, 
                        ins.ondemand, price))
        
        # Now sort all of these instances by price
        sorted_instances = sorted(unsorted_instances, key=lambda k: k.price)

        return sorted_instances

    def print_cheapest_options(self, sorted_instances):
        # Print out the top three
        logger.info("Top three to select from:")
        top_three = 3
        for ins in sorted_instances:
            if top_three == 0:
                break
            logger.info("    %s %s %s" %
                        (ins.instance_type, ins.zone, ins.price))
            top_three = top_three - 1

    def get_timeout_ondemand(self, job, tenant, instances):
        """
        Check to see if the job now requires an ondemand instance due to
        timing out.
        """
        cur_time = datetime.datetime.now()
        cur_time = calendar.timegm(cur_time.timetuple())

        time_idle = cur_time - int(job.req_time)

        res_instance = None
        # if the tenant has set a timeout and the job has been idle longer than
        # this
        if tenant.timeout > 0 and time_idle > tenant.timeout:
            # sort the eligibile instances by their ondemand price (odp)
            sorted_instances = sorted(instances, key=lambda k: k.odp)
            logger.debug("Selecting ondemand instance: %s" %
                         str(job.launch))
            res_instance = sorted_instances[0]
        return res_instance

    def check_ondemand_needed(self, tenant, sorted_instances, job):
        # Check to see if an ondemand instance is required due to timeout
        needed = False
        launch_instance = self.get_timeout_ondemand(job, tenant,
                                                    sorted_instances)
        cheapest = sorted_instances[0]

        # check to see if it timed out
        if (launch_instance is not None and
                launch_instance.odp < tenant.max_bid_price):
            job.launch = aws.Request(
                launch_instance, launch_instance.type, "", launch_instance.ami,
                1, launch_instance.odp, True)
            logger.debug("Selected to launch on demand due to timeout: %s" %
                         str(job.launch))
            needed = True

        # check if the job is flagged as needing on-demand
        elif job.ondemand:
            needed = True

        # if the cheapest option is ondemand
        elif cheapest.ondemand and cheapest.odp < tenant.max_bid_price:
            job.launch = cheapest
            logger.debug("Selected to launch on demand due to ondemand " +
                         "being cheapest: %s" % repr(cheapest))
            needed = True

        # or if the cheapest option close in price to ondemand, then use
        # ondemand.
        elif (cheapest.price >
                (ProvisionerConfig().ondemand_price_threshold *
                    float(cheapest.odp)) and
                cheapest.price < tenant.max_bid_price):
            job.launch = cheapest
            logger.debug("Selected to launch on demand due to spot price " +
                         "being close to ondemand price: %s" %
                         repr(cheapest))
            needed = True

        return needed

    def select_instance_type(self, instances):
        """
        Select the instance to launch for each idle job.
        """
        for tenant in self.tenants:
            for job in list(tenant.idle_jobs):
                # Get the set of instance types that can be used for this job
                eligible_instances = self.restrict_instances(job)
                if len(eligible_instances) == 0:
                    logger.error("Failed to find any eligible instances for job %s" % job)
                    continue
                # get all potential pairs and sort them
                sorted_instances = self.get_potential_instances(
                    eligible_instances, job)
                if len(sorted_instances) == 0:
                    logger.error("Failed to find any sorted instances for job %s" % job)
                    continue

                # work out if an ondemand instance is needed
                job.ondemand = self.check_ondemand_needed(tenant, 
                                                          sorted_instances,
                                                          job)

                # If ondemand is required, redo the sorted list with only
                # ondemand requests and set that to be the launched instance
                if job.ondemand:
                    sorted_instances = self.get_potential_instances(
                        eligible_instances, job)

                    job.launch = sorted_instances[0]
                    logger.debug("Launching ondemand for this job. %s" %
                                 str(job.launch))
                    continue

                # otherwise we are now looking at launching a spot request
                # print out the options we are looking at
                self.print_cheapest_options(sorted_instances)
                # filter out a job if it has had too many requests made
                existing_requests = self.get_existing_requests(tenant, job)
                if len(existing_requests) >= ProvisionerConfig().max_requests:
                    logger.debug(("Too many requests already exist " +
                                  "for this job: %s") % job.id)
                    tenant.idle_jobs.remove(job)
                    continue

                # Find the top request that hasn't already been requested
                # (e.g. zone+type pair is not in existing_requests)
                for req in sorted_instances:
                    if len(existing_requests) > 0:
                        # Skip this type if a matching request already
                        # exists
                        exists = False
                        for existing in existing_requests:
                            logger.debug("Comparing existing requests:")
                            logger.debug("%s vs %s" % (req.instance_type, 
                                                       existing.instance_type))
                            logger.debug("%s vs %s" % (req.zone, 
                                                       existing.zone))
                            if (req.instance_type == existing.instance_type and
                                    req.zone == existing.zone):
                                exists = True
                        if exists:
                            continue
                    # Launch this type. 
                    if req.price < tenant.max_bid_price:
                        req.bid = self.get_bid_price(job, tenant, req)
                        job.launch = req
                        logger.debug("Selecting instance: %s" %
                                     str(job.launch))
                        break
                    else:
                        logger.error(("Unable to launch request %s as " +
                                      "the bid is higher than max bid " +
                                      "%s.") % (str(req), tenant.max_bid_price))

    def get_existing_requests(self, tenant, job):
        # Get all of the outstanding requests from the db for this instance
        existing_requests = []
        try:
            rows = ProvisionerConfig().dbconn.execute(
                ("select instance_request.instance_type, " +
                 "instance_request.request_type, " +
                 "instance_type.type, " +
                 "instance_request.subnet, subnet_mapping.zone " +
                 "from instance_request, subnet_mapping, instance_type " +
                 "where job_runner_id = %s and " +
                 "instance_request.tenant = %s and " +
                 "instance_request.instance_type = instance_type.id and "
                 "subnet_mapping.id = instance_request.subnet") %
                (job.id, tenant.db_id))
            for row in rows:
                existing_requests.append(aws.Request(
                    None, row['type'],
                    row['zone'], None, None))
        except psycopg2.Error:
            logger.exception("Error getting number of outstanding")

        return existing_requests

    def restrict_instances(self, job):
        """
        Filter out instances that do not meet the requirements of a job then
        return a list of the eligible instances.
        """
        eligible_instances = []

        # Check if the instance is viable for the job
        instance_types = ProvisionerConfig().instance_types
        for instance in instance_types:
            # logger.debug("type:%s cpus:%s mem:%s -- req cpus:%s req_mem:%s" %
                         # (instance.type, instance.cpus, instance.memory,
                          # job.req_cpus, job.req_mem))
            if aws.manager.check_requirements(instance, job):
                eligible_instances.append(instance)

        # Print out the eligible instances
        logger.debug("Eligibile instances:")
        for ins in eligible_instances:
            logger.debug(ins.type)

        return eligible_instances

    def get_bid_price(self, job, tenant, req):
        """
        This function is not totally necessary at the moment, but it could be
        expanded to include more complex logic when placing a bid.
        Currently it just does bid percent * ondemand price of the resource
        and checks it is less than the maximum bid.
        """

        bid = float(tenant.bid_percent) / 100 * float(req.odp)
        if bid <= tenant.max_bid_price:
            return bid
        else:
            return 0.40
