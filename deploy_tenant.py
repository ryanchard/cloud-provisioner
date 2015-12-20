#!/usr/bin/env python

import boto
import psycopg2
import sys
import sqlalchemy
import ConfigParser
import subprocess
import json

"""
This script is used to put the tenant's gateway information in to
the provisioner's base. The script goes through the instances
metadata to pull out local addresses and security_groups etc.
It also uses the aws CLI to get the subnet mapping information.
Once all of this information is retrieved and put in the database,
the provisioner should be able to monitor the gateway's condor queue
(once the condor config file includes the provisioner's address) to
acquire resources for any jobs.
"""


class AddProvisioner():

    def __init__(self):
        config_file = 'deploy.ini'
       
        # read config from a file
        config = ConfigParser.ConfigParser()
        config.read(config_file)

        # get DB connection info
        user = config.get('Database', 'user')
        password = config.get('Database', 'password')
        host = config.get('Database', 'host')
        port = config.get('Database', 'port')
        database = config.get('Database', 'database')

        # create a connection and keep it as a config attribute
        try:
            engine = sqlalchemy.create_engine(
                'postgresql://%s:%s@%s:%s/%s' %
                (user, password, host, port, database))
            self.dbconn = engine.connect()
        except psycopg2.Error:
            logger.exception("Failed to connect to database.")

        # Get the AWS credentials
        self.access_key = config.get('AWS', 'access_key')
        self.secret_key = config.get('AWS', 'secret_key')

        # Now load the provisioner specific details
        self.provisioner = config.get('Provisioner', 'provisioner')
        self.name = config.get('Provisioner', 'name')
        self.domain = config.get('Provisioner', 'domain')
        self.security_group = config.get('Provisioner', 'security_group')
        self.scheduler_type = config.get('Provisioner', 'scheduler_type')
        self.subscribed = config.get('Provisioner', 'subscribed')
        self.max_bid_price = config.get('Provisioner', 'max_bid_price')
        self.bid_percent = config.get('Provisioner', 'bid_percent')
        self.timeout_threshold = config.get('Provisioner', 
                                            'timeout_threshold')
        self.custom_ami = config.get('Provisioner', 'custom_ami')
        if self.custom_ami == "None":
            self.custom_ami = ""
        self.key_pair = ""


    def run(self):
        """
        Add this gateway's details to the provisioner database. This will need
        to work out the details of the instance it is executed on and insert
        them in the shared provisioner database.
        """



        print 'LOADING METADATA'
        # Start by pulling all of the instances details from AWS 
        self.load_aws_metadata()

        print 'CHECKING IF IT ALREADY EXISTS'
        # Check if this tenant is already in the database. If it is
        # then skip 
        exists = self.check_exists()
        if exists:
            print 'TENANT ALREADY IN DATABASE'
            return

        print 'LOADING AWS SUBNETS'
        # Then grab all of the subnet information
        self.load_aws_subnets()
        print 'INSERTING CREDENTIALS'
        # Insert credentials if they don't already exist
        self.credential_id = self.insert_credentials()
        print 'CREDENTIALS = %s' % self.credential_id

        print 'INSERTING TENANT'
        # Insert the tenant into the database
        self.tenant_id = self.insert_tenant()
        print 'TENANT = %s' % self.tenant_id

        print 'INSERTING SUBNETS'
        # Insert the subnet details
        self.insert_subnets()

        print 'INSERTING SETTINGS'
        # Insert tenant settings
        self.insert_settings()

        print 'MODIFYING CONDOR CONFIG'
        # Modify the condor local config file.
        #self.modify_condor_config()


    def check_exists(self):
        """
        Check if the tenant already exists in the database
        """

        # The easiest way is to check if the hostname is already in the DB.
        cmd = ("SELECT * from tenant where public_address " + 
              "= '%s'" % self.public_address)
        
        rows = self.dbconn.execute(cmd)
        # If it is already there, just return that id
        for row in rows:
            return True

        return False


    def load_aws_metadata(self):
        """
        Load in all of the instance details using boto to get the metadata.
        """

        conn = boto.connect_ec2(self.access_key, self.secret_key)
        
        # get the local hostname
        metadata = boto.utils.get_instance_metadata()
        self.hostname = metadata['local-hostname']
        self.public_address = metadata['public-hostname']
        self.local_ipv4 = metadata['local-ipv4']

        # Get the zone of the gateway
        self.zone = metadata['placement']['availability-zone']
        # This caused an error, check if this fixes...
        self.zone = 'us-east-1c'
        # Get VPC details.
        mac = metadata['network']['interfaces']['macs']
        vpc_id = 0
        for key, val in mac.iteritems():
            self.vpc_id = mac[key]['vpc-id']
            break


        print self.hostname
        print self.public_address
        print self.local_ipv4
        print self.zone 
        print self.vpc_id

    def load_aws_subnets(self):
        """
        Now get the subnet details
        I can't seem to find a great example of how to do this with boto.
        The examples seem to create a VPC connection, which I don't think
        is what we want. I also can't seem to find them in the meta data.
        All we want to do is use 'aws ec2 describe-subnets', so bugger it,
        lets just do that.
        """

        cmd = ['aws', 'ec2', 'describe-subnets']
        out = subprocess.Popen(cmd, stdout=subprocess.PIPE).communicate()[0]
        json_res = json.loads(out)
        print json_res

        self.subnet_mapping = []

        for key, val in json_res.iteritems():
            for vpc in val:
                # Check if 'worker-subnet-*' is in the tags. Otherwise it will
                # capture the headnode subnets as well.
                if 'Tags' in vpc and 'work' in vpc['Tags'][0]['Value']:
                    mapping = {'zone' : vpc['AvailabilityZone'],
                                'subnet' : vpc['SubnetId']}
                    print 'adding map %s' % mapping
                    self.subnet_mapping.append(mapping)
        print self.subnet_mapping


    def insert_credentials(self):
        """
        Add the credentials to the database. If they are already in there,
        just return the id.
        """

        # First check if this key paid is already in there
        cmd = ("SELECT id from aws_credentials where access_key_id " + 
              "= '%s'" % self.access_key)

        rows = self.dbconn.execute(cmd)
        # If it is already there, just return that id
        for row in rows:
            return row['id']

        # If there wasn't already one in there, insert a new set and return
        # the id
        cmd = ("INSERT INTO aws_credentials (access_key_id, secret_key, " +
              "key_pair) values ('%s', '%s', '%s') RETURNING id" % 
              (self.access_key, self.secret_key, self.key_pair))

        rows = self.dbconn.execute(cmd)

        for row in rows:
            return row['id']

        return 0

    def insert_tenant(self):
        """
        Insert the tenant information in to the database.
        """


        cmd = (("INSERT INTO tenant (name, public_address, condor_address, " +
               "public_ip, zone, vpc, domain, credentials, subscribed, " +
               "security_group) " +
               "values ('%s','%s','%s','%s','%s','%s','%s','%s',%s" +
               ",'%s') RETURNING id") % 
               (self.name, self.public_address, self.hostname, 
               self.local_ipv4, self.zone, self.vpc_id, self.domain, 
               self.credential_id, self.subscribed, self.security_group))
# For deploying with Spark where scheduler type is necessary
#        cmd = (("INSERT INTO tenant (name, public_address, condor_address, " +
#               "public_ip, zone, vpc, domain, credentials, subscribed, " +
#               "security_group, scheduler_type) " +
#               "values ('%s','%s','%s','%s','%s','%s','%s','%s',%s,'%s'" +
#               ",'%s') RETURNING id") % 
#               (self.name, self.public_address, self.hostname, 
#               self.local_ipv4, self.zone, self.vpc_id, self.domain, 
#               self.credential_id, self.subscribed, self.security_group, 
#               self.scheduler_type))

        rows = self.dbconn.execute(cmd)

        for row in rows:
            return row['id']


    def insert_subnets(self):
        """
        Insert each of the subnet mappings in to the database
        """
        for subnet in self.subnet_mapping:
            cmd = (("INSERT INTO subnet_mapping (tenant, zone, " +
                   "subnet) values (%s,'%s','%s') ")  % 
                  (self.tenant_id, subnet['zone'], subnet['subnet']))

            self.dbconn.execute(cmd)


    def insert_settings(self):
        """
        Insert details into the settings table.
        """
        cmd = (("INSERT INTO tenant_settings (tenant, max_bid_price, " +
               "bid_percent, timeout_threshold) VALUES " +
               "(%s,'%s',%s,%s)")  % 
              (self.tenant_id, self.max_bid_price, self.bid_percent, 
              self.timeout_threshold))
# Custom ami is also for Spark.
#        cmd = (("INSERT INTO tenant_settings (tenant, max_bid_price, " +
#               "bid_percent, timeout_threshold,  custom_ami) VALUES " +
#               "(%s,'%s',%s,%s,'%s')")  % 
#              (self.tenant_id, self.max_bid_price, self.bid_percent, 
#              self.timeout_threshold, self.custom_ami))

        rows = self.dbconn.execute(cmd)


    def modify_condor_config(self):

        for line in fileinput.input("/etc/condor/condor_config.local", 
                                    inplace=True):
            line = re.sub('CONDOR_HOST = .*',"CONDOR_HOST = %s" % 
                          self.provisioner, line.rstrip())
            print line

        #turn off condor
        submit = subprocess.Popen((["condor_off", "-peaceful"]), 
                                   stdout=subprocess.PIPE, 
                                   stderr=subprocess.STDOUT )
        s_out, s_err = submit.communicate()
        print s_out

        #restart condor on the new node
        submit = subprocess.Popen((["service", "condor", "restart"]), 
                                   stdout=subprocess.PIPE, 
                                   stderr=subprocess.STDOUT )
        s_out, s_err = submit.communicate()
        print s_out


if __name__ == '__main__':
    adder = AddProvisioner()
    adder.run()


