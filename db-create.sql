CREATE TABLE IF NOT EXISTS aws_credentials(
id serial primary key,
access_key_id varchar(255) not null,
secret_key varchar(255) not null,
key_pair varchar(255)
);

CREATE TABLE IF NOT EXISTS tenant(
id serial primary key,
name varchar(255) UNIQUE not null,
public_address varchar(255) not null,
condor_address varchar(255) not null,
public_ip varchar(255) not null,
zone varchar(255) not null,
vpc varchar(255) not null,
security_group varchar(255) not null,
domain varchar(255),
credentials integer not null,
subscribed boolean default True,
CONSTRAINT fk1_cred FOREIGN KEY (credentials) REFERENCES aws_credentials (id) MATCH SIMPLE ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS subnet_mapping(
id serial primary key,
tenant integer not null,
zone varchar(255) not null,
subnet varchar(255) not null,
CONSTRAINT fk1_sub FOREIGN KEY (tenant) REFERENCES tenant (id) MATCH SIMPLE ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS tenant_settings(
id serial primary key,
tenant integer not null,
max_bid_price numeric default 0 CONSTRAINT positive_price CHECK (max_bid_price >= 0),
bid_percent integer default 80 CONSTRAINT positive_percent CHECK (bid_percent >= 0),
timeout_threshold integer default 0 CONSTRAINT positive_timeout CHECK (timeout_threshold >= 0),
CONSTRAINT fk1_tenant FOREIGN KEY (tenant) REFERENCES tenant (id) MATCH SIMPLE ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS instance_type(
id serial primary key,
type varchar(255) not null,
ondemand_price numeric default 0 CONSTRAINT positive_ondemand_price CHECK (ondemand_price >= 0),
cpus integer default 0 CONSTRAINT positive_cpu_price CHECK (cpus >= 0),
memory numeric default 0 CONSTRAINT positive_memory CHECK (memory >= 0),
disk numeric default 0 CONSTRAINT positive_disk CHECK (disk >= 0),
ami varchar(255) not null,
virtualization varchar(255),
available boolean default True
);

CREATE TABLE IF NOT EXISTS instance_request(
id bigserial primary key,
tenant integer not null,
instance_type integer not null,
price numeric CONSTRAINT positive_price_check CHECK (price >= 0),
job_runner_id integer,
request_type varchar(255) not null,
request_id varchar(255) not null,
request_time timestamp default now(),
subnet integer not null,
CONSTRAINT fk2_tenant FOREIGN KEY (tenant) REFERENCES tenant (id) MATCH SIMPLE ON UPDATE CASCADE ON DELETE CASCADE,
CONSTRAINT fk1_instance FOREIGN KEY (instance_type) REFERENCES instance_type (id) MATCH SIMPLE ON UPDATE CASCADE ON DELETE CASCADE,
CONSTRAINT fk1_subnetid FOREIGN KEY (subnet) REFERENCES subnet_mapping (id) MATCH SIMPLE ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS instance(
id bigserial primary key,
request_id int not null,
instance_id varchar(255) not null,
public_dns varchar(255) not null,
private_dns varchar(255) not null,
fulfilled_time timestamp default now(),
terminate_time timestamp,
cost numeric CONSTRAINT positive_price_check CHECK (cost >= 0),
CONSTRAINT fk1_request FOREIGN KEY (request_id) REFERENCES instance_request (id) MATCH SIMPLE ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS request_migration(
id serial primary key,
request_id int not null,
from_job integer not null,
to_job integer not null,
migration_time timestamp default now(),
CONSTRAINT fk2_request FOREIGN KEY (request_id) REFERENCES instance_request (id) MATCH SIMPLE ON UPDATE CASCADE ON DELETE CASCADE
);
