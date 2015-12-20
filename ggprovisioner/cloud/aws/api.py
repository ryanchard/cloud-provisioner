import boto
import psycopg2
from pytz import timezone
import datetime
import time
from string import Template
from boto.ec2.blockdevicemapping import BlockDeviceType
from boto.ec2.blockdevicemapping import BlockDeviceMapping

from ggprovisioner import ProvisionerConfig, logger


def get_spot_prices(instances, tenant):
    """
    Get the current spot price for each instance type.
    """
    utc = timezone('UTC')
    utc_time = datetime.datetime.now(utc)
    now = utc_time.strftime('%Y-%m-%d %H:%M:%S')
    conn = boto.connect_ec2(tenant.access_key, tenant.secret_key)
    jobCost = 0
    timeStr = str(now).replace(" ", "T") + "Z"
    for ins in instances:
        prices = conn.get_spot_price_history(
            instance_type=ins.type, product_description="Linux/UNIX",
            end_time=timeStr, start_time=timeStr)
        for price in prices:
            ins.spot.update({price.availability_zone: price.price})


def tag_requests(req, tag, conn):
    """
    Tag any requests that have just been made with the tenant name
    """
    for x in range(0, 3):
        try:
            conn.create_tags([req], {"tenant": tag})
            conn.create_tags([req], {"Name": 'worker@%s' % tag})
            break
        except boto.exception.BotoClientError as e:
            time.sleep(2)
            pass
        except boto.exception.BotoServerError as e:
            time.sleep(2)
            pass
        except boto.exception.EC2ResponseError:
            logger.exception("There was an error communicating with EC2.")


def launch_ondemand_request(conn, request, tenant, job):
    try:

        mapping = BlockDeviceMapping()
        sda1 = BlockDeviceType()
        eph0 = BlockDeviceType()
        eph1 = BlockDeviceType()
        eph2 = BlockDeviceType()
        eph3 = BlockDeviceType()
        sda1.size = 10
        eph0.ephemeral_name = 'ephemeral0'
        eph1.ephemeral_name = 'ephemeral1'
        eph2.ephemeral_name = 'ephemeral2'
        eph3.ephemeral_name = 'ephemeral3'
        mapping['/dev/sda1'] = sda1
        mapping['/dev/sdb'] = eph0
        mapping['/dev/sdc'] = eph1
        mapping['/dev/sdd'] = eph2
        mapping['/dev/sde'] = eph3

        # issue a run_instances command for this request
        res = conn.run_instances(
            min_count=request.count, max_count=request.count,
            key_name=tenant.key_pair, image_id=request.ami,
            security_group_ids=[tenant.security_group],
            user_data=customise_cloudinit(tenant, job),
            instance_type=request.instance_type,
            subnet_id=tenant.subnet,
            block_device_map=mapping)
        instances = res.instances
        address = ""
        my_req_ids = [req.id for req in res.instances]
        address = ""
        for req in my_req_ids:
            # tag each request
            tag_requests(req, tenant.name, conn)
            # update the database to include the new request
            ProvisionerConfig().dbconn.execute(
                ("insert into instance_request (tenant, instance_type, " +
                 "price, job_runner_id, request_type, request_id, " +
                 "subnet) values ('%s', '%s', %s, %s, '%s', '%s', %s)") %
                (tenant.db_id, request.instance.db_id, 
                 request.instance.ondemand, job.id,
                 "ondemand", req, tenant.subnet_id))
            ProvisionerConfig().dbconn.commit()
            return
    except boto.exception.EC2ResponseError:
        logger.exception("There was an error communicating with EC2.")


def launch_spot_request(conn, request, tenant, job):
    try:
        logger.debug("%s = %s. tenants vpc = %s" %
                     (request.zone, tenant.subnets[request.zone],
                      tenant.vpc))

        mapping = BlockDeviceMapping()
        sda1 = BlockDeviceType()
        eph0 = BlockDeviceType()
        eph1 = BlockDeviceType()
        eph2 = BlockDeviceType()
        eph3 = BlockDeviceType()
        sda1.size = 10
        eph0.ephemeral_name = 'ephemeral0'
        eph1.ephemeral_name = 'ephemeral1'
        eph2.ephemeral_name = 'ephemeral2'
        eph3.ephemeral_name = 'ephemeral3'
        mapping['/dev/sda1'] = sda1
        mapping['/dev/sdb'] = eph0
        mapping['/dev/sdc'] = eph1
        mapping['/dev/sdd'] = eph2
        mapping['/dev/sde'] = eph3

        inst_req = conn.request_spot_instances(
            price=request.bid, image_id=request.ami,
            subnet_id=tenant.subnets[request.zone], count=request.count,
            key_name=tenant.key_pair,
            security_group_ids=[tenant.security_group],
            instance_type=request.instance_type,
            user_data=customise_cloudinit(tenant, job),
            block_device_map=mapping)
        my_req_ids = [req.id for req in inst_req]
        address = ""
        for req in my_req_ids:
            # tag each request
            tag_requests(req, tenant.name, conn)
            ProvisionerConfig().dbconn.execute(
                ("insert into instance_request (tenant, instance_type, " +
                 "price, job_runner_id, request_type, request_id, " +
                 "subnet) values ('%s', '%s', %s, %s, '%s', '%s', %s)") %
                (tenant.db_id, request.instance.db_id, request.bid, job.id,
                 "spot", req, tenant.subnets_db_id[request.zone]))
            ProvisionerConfig().dbconn.commit()
        return my_req_ids
    except boto.exception.EC2ResponseError:
        logger.exception("There was an error communicating with EC2.")


def request_resources(tenant):
    """
    Request the resources that have been selected for each job
    """
    conn = boto.connect_ec2(tenant.access_key, tenant.secret_key)

    output_string = "Name: %s\n" % tenant.name
    output_string = "%sTenant: %s\n" % (output_string, tenant.name)
    instance_req_string = ""
    req_cpus = 0
    req_instances = 0

    for job in tenant.idle_jobs:
        if job.fulfilled is False:
            request = job.launch
            if request == None:
                logger.debug("Failed to find request object for job %s" % job)
                continue
            logger.debug(repr(request))
            # increment some counters
            req_instances += int(request.count)
            req_cpus += int(job.req_cpus)
            # Launch any on-demand requests
            req_type = "spot"
            if request.ondemand:
                # launch the ondemand request
                launch_ondemand_request(conn, request, tenant, job)
                instance_req_string = (
                    ("%sONDEMAND_INSTANCE_REQUEST" +
                     "\t%s\t%s\t%s\t%s\t%s\n") %
                    (instance_req_string, tenant.name,
                     request.instance_type, request.bid, job.id,
                     "ondemand"))
            else:
                # launch the spot request
                # TODO batch request instances of the same type
                req_ids = launch_spot_request(conn, request, tenant, job)
                for req in req_ids:
                    instance_req_string = (
                        ("%sSPOT_INSTANCE_REQUEST" +
                         "\t%s\t%s\t%s\t%s\t%s\t%s\n") %
                        (instance_req_string, tenant.name,
                         request.instance_type, request.bid, job.id,
                         "spot", req))

    logger.debug(
        ("%s\nTotal CPUs requested: %s\n" +
         "Total instances requested:%s\n%s") %
        (output_string, req_cpus, req_instances, instance_req_string))


def customise_cloudinit(tenant, job):
    """
    Use a string template to construct an appropriate cloudinit script to
    pass as userdata to the aws request.
    """
    cpus = job.launch.instance.cpus
    ip_addr = tenant.public_ip
    domain = tenant.domain
    d = {'ip_addr': ip_addr, 'cpus': cpus, 'domain': domain}

    filein = open(ProvisionerConfig().cloudinit_file)
    src = Template(filein.read())

    result = src.substitute(d)
    return result
