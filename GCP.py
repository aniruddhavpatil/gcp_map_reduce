#!/usr/bin/env python


import argparse
import os
import time
import sys
import pprint

import googleapiclient.discovery
from six.moves import input

class CloudInterface:
    def __init__(self, project, zone):
        self.compute = googleapiclient.discovery.build('compute', 'beta')
        self.project = project
        self.zone = zone
        self.instances = {}

    def list_instances(self):
        result = self.compute.instances().list(project=self.project, zone=self.zone).execute()
        return result['items'] if 'items' in result else None


    def list_images(self):
        result = self.compute.images().list(project=self.project).execute()
        return result['items'] if 'items' in result else None

    def get_ip_from_name(self, name, NAT=False):
        self.update_instances()
        try:
            if NAT:
                return self.instances[name]['networkInterfaces'][0]['accessConfigs'][0]['natIP']
            else:
                return self.instances[name]['networkInterfaces'][0]['networkIP']
        except:
            print('Something went wrong fetching IP')
            return None


    def list_machine_images(self):
        result = self.compute.machineImages().list(project=self.project).execute()
        return result['items'] if 'items' in result else None

    def update_instances(self):
        result = self.list_instances()
        if result is not None:
            for instance in result:
                self.instances[instance['name']] = instance


    def create_instance(self, name, base_image="master-image", preemptible=False, init_script="master.sh"):
        image_response = self.compute.machineImages().get(project=self.project, machineImage=base_image).execute()
        source_disk_image = image_response['selfLink']
        # Configure the machine
        machine_type = "zones/%s/machineTypes/e2-micro" % self.zone
        startup_script = open(os.path.join(os.getcwd(), 'scripts', init_script), 'r').read()
        image_url = "http://storage.googleapis.com/gce-demo-input/photo.jpg"
        image_caption = "Ready for dessert?"

        config = {
            'name': name,
            'machineType': machine_type,

            # Specify the boot disk and the image to use as a source.
            # 'disks': [
            #     {
            #         'boot': True,
            #         'autoDelete': True,
            #         'initializeParams': {
            #             'sourceImage': source_disk_image,
            #         }
            #     }
            # ],

            # Specify a network interface with NAT to access the public
            # internet.
            'networkInterfaces': [{
                'network': 'global/networks/default',
                'accessConfigs': [
                    {'type': 'ONE_TO_ONE_NAT', 'name': 'External NAT'}
                ]
            }],

            # Allow the instance to access cloud storage and logging.
            # 'serviceAccounts': [{
            #     'email': 'default',
            #     'scopes': [
            #         'https://www.googleapis.com/auth/devstorage.read_write',
            #         'https://www.googleapis.com/auth/logging.write'
            #     ]
            # }],
            # Metadata is readable from the instance and allows you to
            # pass configuration from deployment scripts to instances.
            'metadata': {
                'items': [{
                    # Startup script is automatically executed by the
                    # instance upon startup.
                    'key': 'startup-script',
                    'value': startup_script
                }]
            }
        }
        if preemptible:
            config['scheduling'] = {
                'preemptible': True,
                'automaticRestart': False,
                'onHostMaintenance': 'TERMINATE'
            }

        print("Creating Instance with config:")
        pprint.pprint(config, indent=4)

        return self.compute.instances().insert(
            project=self.project,
            zone=self.zone,
            sourceMachineImage=source_disk_image,
            body=config).execute()

        # return compute.instances().insert(
        #     project=project,
        #     zone=zone,
        #     body=config).execute()
    # [END create_instance]


    # [START delete_instance]
    def delete_instance(self, name):
        return self.compute.instances().delete(
            project=self.project,
            zone=self.zone,
            instance=name).execute()
    # [END delete_instance]


    # [START wait_for_operation]
    def wait_for_operation(self, operation):
        print('Waiting for operation to finish...')
        while True:
            result = self.compute.zoneOperations().get(
                project=self.project,
                zone=self.zone,
                operation=operation).execute()

            if result['status'] == 'DONE':
                print("done.")
                if 'error' in result:
                    raise Exception(result['error'])
                return result

            time.sleep(1)
# [END wait_for_operation]


# [START run]
def main(project, zone, wait=True):
    # compute = googleapiclient.discovery.build('compute', 'beta')
    gcp = CloudInterface(project, zone)
    instances = [
        {
            "name": "store",
            "image": "store-image",
            "init-script": "store.sh"
        },
        {
            "name": "master",
            "image": "master-image",
            "init-script": "master.sh"
        },
    ]
    print('Creating instances.')
    for instance in instances:
        operation = gcp.create_instance(instance['name'], instance['image'], False, instance['init-script'])
        gcp.wait_for_operation(operation['name'])

    instances = gcp.list_instances()

    print('Instances in project %s and zone %s:' % (project, zone))
    for instance in instances:
        print(' - ' + instance['name'], "NetworkIP:", gcp.get_ip_from_name(instance['name']), "NAT IP:",gcp.get_ip_from_name(instance['name'], True))

    print("Instances created.")

    if wait:
        input()

    print('Deleting instance.')

    for instance in instances:
        operation = gcp.delete_instance(instance['name'])
        gcp.wait_for_operation(operation['name'])


if __name__ == '__main__':
    main('mapreduce-293315', 'us-central1-f')