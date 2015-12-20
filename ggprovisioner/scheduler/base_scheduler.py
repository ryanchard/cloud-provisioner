import subprocess
import datetime
import calendar
import boto
import psycopg2

from ggprovisioner import logger, ProvisionerConfig
from ggprovisioner.cloud import aws
from ggprovisioner.scheduler import Job

class BaseScheduler():

    def load_jobs(self, tenants):
        """
        Read in the condor queue and manage the removal of jobs that should
        not be processed.
        """
        # Assess the global queue
        all_jobs = self.get_global_queue()

        # Assoicate the jobs from the global queue with each of the tenants
        self.process_global_queue(all_jobs, tenants)

        # Remove any jobs that should not be processed this time. For example,
        # if they have already had an instance fulfilled or have had too many
        # requests made.
        ignore_fulfilled_jobs(tenants)

        # Stop resources being requested too frequently
        stop_over_requesting(tenants)


    def get_global_queue(self):
        """
        Poll all queues and return a set of Jobs.
        """
        pass


    def get_status(self, pool):
        """
        Poll the collector of a pool to get the status, describing the
        resources in the pool.
        """
        return ""


    def process_global_queue(self, jobs, tenants):
        """
        Associate each job with a tenant and add them to their local list of
        jobs.
        """
        pass


#TODO perhaps we should move these two functions to the resource manager?
def ignore_fulfilled_jobs(tenants):
    """
    Check whether a job's spot requests have been fulfilled yet. If so,
    remove the job from the idle_jobs list. Also check whether any
    outstanding, but still valid, request exists. If there are other
    requests for the job, migrate or cancel them.
    """

    for tenant in tenants:
        # Check to see if any entries have been made in the instance table
        # this indicates an instance has been fulfilled for a request.
        # Restrict the query to only looking at requests for a specific job
        for job in list(tenant.idle_jobs):
            rows = ProvisionerConfig().dbconn.execute(
                   ("select instance_request.job_runner_id, " +
                    "instance_type.cpus from instance_request, " +
                    "instance_type, instance where " +
                    "instance_type.id = instance_request.instance_type " +
                    "and instance.request_id = instance_request.id " +
                    "and instance_request.job_runner_id = %s " +
                    "and tenant = %s") % (job.id, tenant.db_id))
            logger.debug(("select instance_request.job_runner_id, " +
                    "instance_type.cpus from instance_request, " +
                    "instance_type, instance where " +
                    "instance_type.id = instance_request.instance_type " +
                    "and instance.request_id = instance_request.id " +
                    "and instance_request.job_runner_id = %s " +
                    "and tenant = %s") % (job.id, tenant.db_id))

            fulfilled_cpus = 0
            # Iterate over the fulfilled instance requests for this job
            for row in rows:
                # Work out how many cpus this instance type has
                fulfilled_cpus = fulfilled_cpus + int(row['cpus'])
              
            # If enough cpus have been acquired, flag the job as fulfilled
            if fulfilled_cpus >= int(job.req_cpus):
                job.fulfilled = True

            # Also remove any that have an ondemand instance fulfilled
            rows = ProvisionerConfig().dbconn.execute(
                   ("select instance_request.job_runner_id " +
                    "from instance_request, instance where " +
                    "instance.request_id = instance_request.id " +
                    "and instance_request.job_runner_id = %s " +
                    "and tenant = %s and instance_request.request_type = " +
                    "'ondemand'") % (job.id, tenant.db_id))
            for row in rows:
                job.fulfilled = True
        # Remove any jobs that have been set as fulfilled from the idle
        # queue
        for job in list(tenant.idle_jobs):
            if job.fulfilled:
                logger.debug("Removing job from idle jobs: %s" %
                             repr(job))
                tenant.idle_jobs.remove(job)

def stop_over_requesting(tenants):
    """
    Stop too many requests being made for an individual job. This is the
    frequency of new requests being made for an individual job.
    Future work would be to look at launching many requests instantly, and
    then cancelling requests once one is fulfilled.
    """
    for tenant in tenants:
        # Stop excess instances being requested in a five minute round
        logger.debug("Tenant: %s. Request rate: %s" %
                     (tenant.name, tenant.request_rate))
        for job in list(tenant.idle_jobs):
            # check to see if we are requesting too frequently
            count = 0
            try:
                rows = ProvisionerConfig().dbconn.execute(
                    ("select count(*) from instance_request " +
                     "where job_runner_id = %s and " +
                     "request_time >= Now() - " +
                     "'%s second'::interval and tenant = %s;") %
                    (job.id, tenant.request_rate, tenant.db_id))
                for row in rows:
                    count = row['count']
            except psycopg2.Error:
                logger.exception("Error getting number of outstanding " +
                                 "requests within time frame.")
            logger.debug("Checking for valid outstanding requests. " +
                         "Count of requests in db = %s" % count)
            if count > 0:
                tenant.idle_jobs.remove(job)
                logger.debug("Removed job %s" % job.id)
                continue

            # now check to see if we already have too many requests for
            # this job
            try:
                rows = ProvisionerConfig().dbconn.execute(
                    ("select count(*) from instance_request " +
                     "where job_runner_id = %s and tenant = %s;") %
                    (job.id, tenant.db_id))
                count = 0
                for row in rows:
                    count = row['count']
                if count > ProvisionerConfig().max_requests:
                    logger.warn("Too many outstanding requests, " +
                                "removing idle job: %s" % repr(job))
                    tenant.idle_jobs.remove(job)
            except psycopg2.Error:
                logger.exception("Error getting number of outstanding " +
                                 "requests.")
