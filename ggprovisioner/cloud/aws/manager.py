import boto
import psycopg2
import sys
import copy
import datetime

from ggprovisioner import logger, ProvisionerConfig
from ggprovisioner.cloud.aws import api


def process_resources(tenants):
    """
    This should manage all of the existing aws resources and requests.
    """
    # Update the DB with newly fulfilled instances
    update_database(tenants)

    # Migrate any requests that still exist for a resource that is not
    # going to use them
    migrate_requests(tenants)

    # Stop any unnecessary spot requests (still launching without any idle
    # jobs)
    cancel_unmigrated_requests(tenants)

    # This function probably isn't needed, but spot requests are scary so
    # this will double check and cancel any requests if the idle queue is
    # empty
    cancel_unnecessary_requests(tenants)


def update_database(tenants):
    """
    Record when an instance is started in the database. This should also
    try and record when an instance is terminated.
    In future work this should probably calculate the cost of the instance
    as well.
    """

    for tenant in tenants:
        conn = boto.connect_ec2(tenant.access_key, tenant.secret_key)
        try:
            # First get all operating instances (instances probably are not
            # yet tagged, so don't filter them yet.)
            reservations = conn.get_all_instances()
            instance_spot_ids = []
            # Go over the fulfilled spot requests
            for r in reservations:
                for i in r.instances:
                    if i.spot_instance_request_id is not None:
                        instance_spot_ids.append(
                            "'%s'" % i.spot_instance_request_id)
                    # Also include ondemand instances which tag as the id.
                    else:
                        instance_spot_ids.append(
                            "'%s'" % i.id)

            # Get the entry in the instance_request table for each of these
            # requests
            check_for_new_instances(reservations, instance_spot_ids,
                                         conn, tenant)
            check_for_terminated_instances(reservations)

        except psycopg2.Error:
            logger.exception("Error updating database.")


def check_for_terminated_instances(reservations):
    for r in reservations:
        for i in r.instances:
            if i.state == 'terminated':
                # Sadly, I can't seem to get the actual shutdown time
                # i.state_reason does not contain it and i.state does not
                # exist. So instead, we will just flag it as now and sort
                # out determining the full hour when computing cost.
                ProvisionerConfig().dbconn.execute(
                    ("update instance set terminate_time = NOW() " +
                     "where instance_id = '%s' and terminate_time is null;") %
                    i.id)


def check_for_new_instances(reservations, instance_spot_ids, conn, tenant):
    if len(instance_spot_ids) > 0:
        # Check that it isn't already in the instance table
        rows = ProvisionerConfig().dbconn.execute(
            ("select instance_request.id, instance_request.job_runner_id, " +
             "instance_request.request_id from instance_request left join " +
             "instance on instance_request.id = instance.request_id where " +
             "instance.request_id is null and instance_request.request_id " +
             "in (%s) and tenant = %s") %
            (",".join(instance_spot_ids), tenant.db_id))

        for row in rows:
            for r in reservations:
                # Match the instance_request entry to an instance request
                # returned from aws
                for i in r.instances:
                    if i.spot_instance_request_id == row['request_id']:
                        # If one is found then update the database
                        instance_acquired(i, row, tenant, conn)
                    if i.id in row['request_id']:
                        instance_acquired(i, row, tenant, conn)


def request_ids_dict(reqs):
    """
    Reorder a set of boto requests into a dict.
    """
    id_to_req = {}
    for r in reqs:
        id_to_req[r.id] = r
    return id_to_req


def migrate_requests(tenants):
    """
    If requests exist for a job that is no longer in the idle queue
    (e.g. it has been fulfilled or scheduled on other resources)
    then migrate any outstanding requests to another job in the idle queue.
    If there are no other jobs in the idle queue, cancel
    all existing requests tagged by a tenant.
    """
    for tenant in tenants:
        conn = boto.connect_ec2(tenant.access_key, tenant.secret_key)
        reqs = conn.get_all_spot_instance_requests(
            filters={"tag-value": tenant.name,
                     "state": "open"})

        # Get a list of ids that can be used in a db query
        ids_to_check = []
        for r in reqs:
            ids_to_check.append("%s" % r.id)

        logger.debug("Open requests: %s" % ids_to_check)

        # Get the set of idle job numbers - using tenant.idle_jobs may not
        # work, as some jobs are removed from the list for various reasons
        # e.g. they have recently had a request made for them, so instead
        # we will use the all_jobs list and check for idle state
        idle_job_numbers = []
        potential_jobs = []
        for job in tenant.jobs:
            if job.status == '1':
                idle_job_numbers.append(job.id)
                potential_jobs.append(job)

        # Get requests that do not belong to idle jobs
        reqs = get_orphaned_requests(tenant, ids_to_check,
                                     idle_job_numbers)

        # if there are any requests, try to reassign them to another job
        if len(reqs) > 0:
            for req in reqs:
                for job in potential_jobs:
                    # try to migrate it. if it works, then go to the next
                    # request. otherwise try the next job.
                    if migrate_request_to_job(req, job):
                        # Remove it from idle jobs so it doesn't also get
                        # a request made for it this round
                        if job in tenant.idle_jobs:
                            tenant.idle_jobs.remove(job)


def get_orphaned_requests(tenant, ids_to_check, idle_job_numbers):
    """
    Check if there are any requests that don't belong to a job in the idle
    queue
    """

    res = []

    if len(ids_to_check) > 0:
        try:
            # Add quotes and commas to the list items for psql.
            sir_ids = (', '.join('\'' + item + '\'' for item in ids_to_check))
            # Get any requests that do not belong to an idle job
            rows = []
            if len(idle_job_numbers) > 0:
                rows = ProvisionerConfig().dbconn.execute(
                    ("select instance_request.id, instance_type.type, " +
                     "instance_request.job_runner_id, " +
                     "instance_request.request_id from instance_request, " +
                     "instance_type where instance_request.instance_type = " +
                     "instance_type.id and job_runner_id not in (%s) and " +
                     "request_id in (%s) and request_type = 'spot' and " +
                     "tenant = %s") %
                    (",".join(idle_job_numbers), sir_ids,
                     tenant.db_id))
            else:
                rows = ProvisionerConfig().dbconn.execute(
                    ("select instance_request.id, instance_type.type, " +
                     "instance_request.job_runner_id, " +
                     "instance_request.request_id from instance_request, " +
                     "instance_type where instance_request.instance_type = " +
                     "instance_type.id and " +
                     "request_id in (%s) and request_type = 'spot' and " +
                     "tenant = %s") % (sir_ids, tenant.db_id))
            # I had some issues with the rows object closing after
            # returning it, so this just builds a dict for it
            for row in rows:
                res.append({'id': row['id'], 'type': row['type'],
                            'job_runner_id': row['job_runner_id'],
                            'request_id': row['request_id']})
                logger.warn("Orphaned request %s" % row['request_id'])

        except psycopg2.Error:
            logger.exception("Error migrating instances.")

    return res


def migrate_request_to_job(request, job):
    """
    Check if an instance can be repurposed to another job and update the
    database.
    """
    # Check to see if the job can be fulfilled by the requested instance
    if check_requirements(request['type'], job):
        next_idle_job_id = job.id
        try:
            logger.debug(
                ("Migrating instance request  %s, from job " +
                 "%s to job %s.") %
                (request['id'], request['job_runner_id'],
                 next_idle_job_id))
            ProvisionerConfig().dbconn.execute(
                ("update instance_request set job_runner_id = '%s' " +
                 "where id = %s") %
                (next_idle_job_id, request['id']))
            ProvisionerConfig().dbconn.execute(
                ("insert into request_migration " +
                 "(request_id, from_job, to_job, migration_time) " +
                 "values (%s, %s, %s, NOW())") %
                (request['id'], request['job_runner_id'],
                 next_idle_job_id))
            return True
        except psycopg2.Error:
            logger.exception("Error performing migration in database.")


def cancel_unmigrated_requests(tenants):
    """
    There are two cases to handle here. Either there are no idle jobs, so
    all requests should be cancelled.
    Or there are idle jobs but the existing requests could not be migrated
    to them. In this case, any orphaned requests should also be cancelled.
    """
    for tenant in tenants:
        # start by grabbing all of the open spot requests for this tenant
        conn = boto.connect_ec2(tenant.access_key, tenant.secret_key)
        reqs = conn.get_all_spot_instance_requests(
            filters={"tag-value": tenant.name, "state": "open"})

        # Get a list of ids that can be used in a db query
        ids_to_check = []
        for r in reqs:
            ids_to_check.append("%s" % r.id)

        # Get the set of idle job numbers
        idle_job_numbers = []
        for job in tenant.jobs:
            if job.status == '1':
                idle_job_numbers.append(job.id)

        # now get all of the orphaned requests
        reqs = get_orphaned_requests(tenant, ids_to_check,
                                     idle_job_numbers)
        reqs_to_cancel = []
        # build a nice list we can pass to boto
        for req in reqs:
            reqs_to_cancel.append(req['request_id'])

        # now cancel all of these requests
        try:
            if len(reqs_to_cancel) > 0:
                logger.debug("Cancelling unmigrated requests: %s" %
                             reqs_to_cancel)
                conn.cancel_spot_instance_requests(ids_to_check)
        except Exception as e:
            logger.exception("Error removing spot instance requests.")
            raise e


# TODO test the cancel unmigrated function and if that is suffcient,
# just delete this one.
def cancel_unnecessary_requests(tenants):
    """
    Make sure spot requests are closed if there are no idle jobs in the
    queue.
    """
    for tenant in tenants:
        # start by grabbing all of the open spot requests for this tenant
        conn = boto.connect_ec2(tenant.access_key, tenant.secret_key)
        reqs = conn.get_all_spot_instance_requests(
            filters={"tag-value": tenant.name, "state": "open"})
        # That should be sufficient, but just because spot requests are
        # scary lets double check and kill anything if there are no idle
        # jobs.
        status = []
        for job in tenant.jobs:
            # Remove any jobs that have now been fulfilled too
            if job.fulfilled == False:
                status.append(job.status)

        # If there are no jobs currently idle, terminate any outstanding
        # spot requests
        if not all(stat == '1' for stat in status) or len(status) == 0:
            # Reorder these requests to a usable array
            id_to_req = request_ids_dict(reqs)
            # Build a list of requests to cancel
            to_cancel = id_to_req.keys()
            # Cancel these requests
            if len(to_cancel) > 0:
                logger.error("This should be deprecated if the other " +
                             "cancel function is working correctly.")
                logger.debug("Cancelling spot requests: %s" % to_cancel)
                conn.cancel_spot_instance_requests(to_cancel)


def instance_acquired(inst, request, tenant, conn):
    """
    A new instance has been acquired, so insert a record into the instance
    table and tag it with the tenant name
    """
    launch_time = datetime.datetime.strptime(inst.launch_time,
                                             "%Y-%m-%dT%H:%M:%S.000Z")
    # insert it into the database
    ProvisionerConfig().dbconn.execute(
        ("insert into instance (request_id, instance_id, fulfilled_time, " +
         "public_dns, private_dns) values ('%s', '%s', '%s', '%s', '%s')") %
        (request['id'], inst.id, launch_time,
         inst.public_dns_name, inst.private_dns_name))
    logger.debug("An instance has been acquired. " +
                 "Tenant={0}; Request={1}, Instance={2}".format(
                     tenant.name, repr(request), repr(inst)))

    # now tag the request
    api.tag_requests(inst.id, tenant.name, conn)

    # if the job is still in the idle queue, we should remove it as the
    # instance was now launched for it
    for job in tenant.jobs:
        logger.debug("Checking {0} vs {1}".format(repr(job), repr(request)))
        if int(job.id) == int(request['job_runner_id']):
            logger.debug("Launched an instance for job %s - removing it." %
                         request['job_runner_id'])
            job.fulfilled = True


def migrate_instance():
    """
    A placeholder for where the migration of instances will fit in to this.
    """
    logger.debug("Migration not yet supported.")


def check_requirements(instance, job):
    """
    Check to see if an instance can fulfil a jobs requirements.
    I came across a problem here where I wanted to use instance_requests
    from the database, and I didn't have an instance type. So it now
    accepts both an Instance or str (aws instance type name
    e.g. m2.4xlarge)
    """
    # This lets it pass in the name of an instance too, e.g. m2.4xlarge,
    # where if it is a str it will look up the instance and use that.
#    if isinstance(instance, basestring):
#        instance_types = ProvisionerConfig().instance_types
#        instance = [ins for ins in instance_types
#                    if ins.type == instance][0]

    if isinstance(instance, basestring):
        for ins in ProvisionerConfig().instance_types:
            if ins.type == instance:
                instance = ins
                break
    # logger.debug(instance)
    # logger.debug(ProvisionerConfig().instance_types)
    # Check it meets cpu requirements
    if int(instance.cpus) < int(job.req_cpus):
        return False
    # Check it meets memory requirements
    if int(instance.memory) < int(job.req_mem):
        return False

    # Restrict by profile ... once this is done.

    # It seems to meet the requirements
    return True
