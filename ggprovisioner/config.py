import ConfigParser

import sqlalchemy
import psycopg2

from ggprovisioner import Singleton, logger


class ProvisionerConfig(object):
    """
    Stateful storage of loaded configuration values in an object. A
    ProvisionerConfig instantiated with its initializer loads config from
    disk and the DB.
    Because this class is a Singleton, multiple attempts to construct it will
    only return one instance of the class
    """
    __metaclass__ = Singleton

    def __init__(self, *args, **kwargs):
        """
        Load provisioner configuration based on the settings in a config file.
        """
        # override defaults with kwargs
        config_file = 'ggprovisioner/provisioner.ini'
        cloudinit_file = "cloudinit.cfg"
        if 'config_file' in kwargs:
            config_file = kwargs['config_file']
        if 'cloudinit_file' in kwargs:
            cloudinit_file = kwargs['cloudinit_file']

        # we need to pull cloudinit from the DB in the future
        self.cloudinit_file = cloudinit_file

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

        # Get some provisioner specific config settings
        self.ondemand_price_threshold = float(
            config.get('Provision', 'ondemand_price_threshold'))
        self.max_requests = int(config.get('Provision', 'max_requests'))
        self.run_rate = int(config.get('Provision', 'run_rate'))

        self.instance_types = []

    def load_instance_types(self):
        """
        Load instance types from database into config object
        """
        # this must be imported here to avoid a circular import
        from ggprovisioner.cloud import aws

        def get_instance_types():
            """
            Get the set of instances from the database
            """
            instances = []
            try:
                rows = self.dbconn.execute(
                    "select * from instance_type where available = True")
                for row in rows:
                    instances.append(aws.Instance(
                        row['id'], row['type'], row['ondemand_price'],
                        row['cpus'], row['memory'], row['disk'], 
                        row['ami']))
            except psycopg2.Error:
                logger.exception("Error getting instance types from database.")
            # logger.debug("The set of instances from the database:")
            # for ins in instances:
            #     logger.debug(repr(ins))
            return instances

        self.instance_types = get_instance_types()
