import psycopg2
import sys

from ggprovisioner import logger, ProvisionerConfig, SimpleStringifiable


class Tenant(SimpleStringifiable):
    """
    A class to maintain tenant information.
    """
    def __init__(self, db_id, name, p_addr, c_addr, ip_addr, zone, subnet, 
                 subnet_id, vpc, security_group, domain, max_bid, bid_percent, 
                 timeout, access_key, secret_key, key_pair):
        self.db_id = db_id
        self.name = name
        self.public_address = p_addr
        self.condor_address = c_addr
        self.public_ip = ip_addr
        self.zone = zone
        self.subnet = subnet
        self.subnet_id = subnet_id
        self.vpc = vpc
        self.security_group = security_group
        self.max_bid_price = max_bid
        self.bid_percent = bid_percent
        self.timeout = timeout
        self.access_key = access_key
        self.secret_key = secret_key
        self.key_pair = key_pair
        self.domain = domain
        subnets = {}
        subnets_db_id = {}

        # TODO Add the option for varying idle times (how long a job must be
        # in queue before being processed) and request rates to the database
        self.idle_time = 10
        self.request_rate = 600
        self.jobs = []
        self.idle_jobs = []


def load_from_db():
    """
    Load all of the tenant data. This should let us iterate over the
    tenants and apply their preferences to the idle queue etc..
    It should also let us shut down unnecessary requests across the board
    as this will load their aws credentials.
    """
    tenant_list = []
    # Pull all of the tenant data from the database
    try:
        # Only get those that are subscribed
        tenant_columns = ['id', 'name', 'public_address', 'condor_address',
                          'public_ip', 'zone', 'vpc', 'security_group',
                          'domain']
        tenant_settings_columns = ['max_bid_price', 'bid_percent',
                                   'timeout_threshold']
        tenant_selection = ', '.join('tenant.' + c for c in tenant_columns)
        tenant_settings_selection = ', '.join('tenant_settings.' + c for c
                                              in tenant_settings_columns)
        aws_creds_columns = ['access_key_id', 'secret_key', 'key_pair']
        aws_creds_selection = ', '.join('aws_credentials.' + c for c in
                                        aws_creds_columns)
        subnet_columns = ['subnet', 'id as subnet_id']
        subnet_selection = ', '.join('subnet_mapping.' + c for c in
                                        subnet_columns)
        full_selection = ', '.join((tenant_selection,
                                    tenant_settings_selection,
                                    aws_creds_selection, subnet_selection))


        rows = ProvisionerConfig().dbconn.execute(
            "SELECT " + full_selection +
            " FROM tenant, tenant_settings, aws_credentials, subnet_mapping" +
            " WHERE tenant_settings.tenant = tenant.id AND" +
            " tenant.credentials = aws_credentials.id AND" +
            " tenant.subscribed = TRUE AND subnet_mapping.tenant" +
            " = tenant.id AND subnet_mapping.zone = tenant.zone")
        # Create a tenant object for each row returned
        for row in rows:
            t = Tenant(row['id'], row['name'], row['public_address'],
                       row['condor_address'], row['public_ip'],
                       row['zone'], row['subnet'], row['subnet_id'],
                       row['vpc'], row['security_group'], row['domain'], 
                       row['max_bid_price'], row['bid_percent'], 
                       row['timeout_threshold'], row['access_key_id'], 
                       row['secret_key'], row['key_pair'])
            # Pull the subnets for the tenant too
            subnets = {}
            subnets_db_id = {}
            subs = ProvisionerConfig().dbconn.execute(
                "select * " +
                "from subnet_mapping " +
                "where tenant = %s" % t.db_id)
            # Create a dict for the subnets and add that to the tenant
            # I later realised that I need the database id of the subnet to
            # store the instance request in the database:
            # Hello, subnets_db_id.
            for sn in subs:
                subnets.update({sn['zone']: sn['subnet']})
                subnets_db_id.update({sn['zone']: sn['id']})
            t.subnets = subnets
            t.subnets_db_id = subnets_db_id
            tenant_list.append(t)
    except psycopg2.Error:
        logger.exception("Failed to get tenant data.")

    return tenant_list
