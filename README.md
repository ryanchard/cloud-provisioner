# README #

This is a provisioning service for the cloud. The goal of this service is to automate the acquisition and management of cloud resources for platforms. In its current state the provisioner is capable of cost-effectively acquiring resources and having them join worker pools. The provisioner has been developed for the Globus Galaxies platform, a set of scientific gateways. HTCondor is primarily supported, so the provisioner monitors HTCondor queues and acquires instances to fulfill workloads.

### Installation ###

Deploying the provisioner is still somewhat complicated. Various aspects of the service have been automated with deployment scripts. For example, to install the provisioner on a host, you will need to complete the deploy.ini config file and execute the deploy_provisioner.py script. This should sort out most of the requirements, including setting up the database with the necessary tables. 

A deployment script for setting the provisioner up on a tenant has also been included. Primarily this will populate the database with information about the tenant and modify the tenant’s HTCondor configuration to report ClassAds to the provisioner.

### Who do I talk to? ###

Any questions, please feel free to email me at ryan@ecs.vuw.ac.nz. 