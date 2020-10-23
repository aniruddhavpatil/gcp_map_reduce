#!/usr/bin/env python


import argparse
import os
import time
import sys
import pprint

import googleapiclient.discovery
from six.moves import input


def list_instances(compute, project, zone):
    result = compute.instances().list(project=project, zone=zone).execute()
    return result['items'] if 'items' in result else None


def list_images(compute, project, zone):
    result = compute.images().list(project=project).execute()
    return result['items'] if 'items' in result else None


def list_machine_images(compute, project):
    result = compute.machineImages().list(project=project).execute()
    return result['items'] if 'items' in result else None


def create_instance(compute, project, zone, name, base_image="master-image", preemptible=False):
    image_response = compute.machineImages().get(project=project, machineImage="store-image").execute()
    source_disk_image = image_response['selfLink']

    # Configure the machine
    machine_type = "zones/%s/machineTypes/e2-micro" % zone
    startup_script = open(
        os.path.join(
            os.path.dirname(__file__), 'startup-script.sh'), 'r').read()
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
    # sys.exit()

    return compute.instances().insert(
        project=project,
        zone=zone,
        sourceMachineImage=source_disk_image,
        body=config).execute()

    # return compute.instances().insert(
    #     project=project,
    #     zone=zone,
    #     body=config).execute()
# [END create_instance]


# [START delete_instance]
def delete_instance(compute, project, zone, name):
    return compute.instances().delete(
        project=project,
        zone=zone,
        instance=name).execute()
# [END delete_instance]


# [START wait_for_operation]
def wait_for_operation(compute, project, zone, operation):
    print('Waiting for operation to finish...')
    while True:
        result = compute.zoneOperations().get(
            project=project,
            zone=zone,
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
    compute = googleapiclient.discovery.build('compute', 'beta')
    # print(list_instances(compute, project, zone))
    # sys.exit()
    instances = [
        {"name": "store", "image": "store-image"},
        {"name": "master", "image": "master-image"},
    ]
    print('Creating instances.')
    for instance in instances:
        operation = create_instance(compute, project, zone, instance['name'], instance['image'])
        wait_for_operation(compute, project, zone, operation['name'])

    instances = list_instances(compute, project, zone)

    print('Instances in project %s and zone %s:' % (project, zone))
    for instance in instances:
        print(' - ' + instance['name'])

    print("Instances created.")

    if wait:
        input()

    print('Deleting instance.')

    for instance in instances:
        operation = delete_instance(compute, project, zone, instance['name'])
        wait_for_operation(compute, project, zone, operation['name'])


if __name__ == '__main__':
    main('mapreduce-293315', 'us-central1-f')