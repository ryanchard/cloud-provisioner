#!/usr/bin/env python

import boto
import psycopg2
import sys
import sqlalchemy
import ConfigParser
import subprocess
import json
import git

"""
This script is used to fully deploy the provisioner on a new instance.
The script should download the provisioner, build it, set up the database
and configure condor. Once everything is configured the provision should be
able to be run.

The required packages are:

apt-get:
update
htcondor
python-psycopg2
python-sqlalchemy
python-boto

pip:
pytz
gitpython

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

        # Now load the deployment details for the provisioner
        self.git_addr = config.get('ProvisionDeploy', 'git')
        self.sql_file = config.get('ProvisionDeploy', 'sql_file')
        self.install_dir = config.get('ProvisionDeploy', 'install_dir')
        self.condor_template = config.get('ProvisionDeploy', 'condor_template')
        


    def run(self):
        """
        Add this gateway's details to the provisioner database. This will need
        to work out the details of the instance it is executed on and insert
        them in the shared provisioner database.
        """

        print 'CLONING GIT REPO AND BUILDING IT'
        # Clone the git repo out and then build it
        self.get_and_build_provisioner()

        print 'MODIFYING CONDOR CONFIG'
        # Modify the condor local config file.
        self.modify_condor_config()

        print 'CREATING DATABASE'
        # Create the database tables if they do not already exist
        self.create_database()


    def get_and_build_provisioner(self):
        """
        Clone the git repository to this machine. Then build the provisioner
        """
        
        # Clone the repo to the install directory
        git.Repo.clone_from(self.git_addr, self.install_dir)

        # Start a subprocess to build the provisioner
        cmd = ['python', 'setup.py', 'install']
        out = subprocess.Popen(cmd, stdout=subprocess.PIPE).communicate()[0]

        print out


    def modify_condor_config(self):
        """
        Write the access control permissions to the condor_config file.
        """
        # Write out the condor access control permissions from the template

        condor_config = "/etc/condor/condor_config.local"
        with open(condor_config, "w+") as f:
            with open(self.condor_template) as c:
                for line in c.readlines():
                    f.write(line)

        # Turn off condor
        submit = subprocess.Popen((["condor_off", "-peaceful"]), 
                                   stdout=subprocess.PIPE, 
                                   stderr=subprocess.STDOUT )
        s_out, s_err = submit.communicate()
        print s_out

        # Restart condor on the new node
        submit = subprocess.Popen((["service", "condor", "restart"]), 
                                   stdout=subprocess.PIPE, 
                                   stderr=subprocess.STDOUT )
        s_out, s_err = submit.communicate()
        print s_out


    def create_database(self):
        """
        Execute the create database sql file. This should create
        each table if it does not already exist.
        """
        for line in open(self.sql_file).read().split(';\n'):
            print 'executing %s' % line
            if len(line) > 5:
                self.dbconn.execute(line)

if __name__ == '__main__':
    adder = AddProvisioner()
    adder.run()


