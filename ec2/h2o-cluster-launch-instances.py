#!/usr/bin/env python

import os
import time
import boto
import boto.ec2


# Environment variables you MUST set (either here or by passing them in).
# -----------------------------------------------------------------------
#
# os.environ['AWS_ACCESS_KEY_ID'] = '...'
# os.environ['AWS_SECRET_ACCESS_KEY'] = '...'


# Options you MUST tailor to your own AWS account.
# ------------------------------------------------

# SSH key pair name.
keyName = '0xdata_Big'

# AWS security group name.
# Note:
#     H2O uses TCP and UDP ports 54321 and 54322.
#     RStudio uses TCP port 8787.
securityGroupName = 'SecurityDisabled'


# Options you might want to change.
# ---------------------------------

numInstancesToLaunch = 2
instanceType = 'm1.large'
instanceNameRoot = 'H2ORStudioDemo'


# Options to help debugging.
# --------------------------

debug = 0
# debug = 1
dryRun = False
# dryRun = True


# Options you should not change unless you really mean to.
# --------------------------------------------------------

regionName = 'us-east-1'
amiId = 'ami-634f050a'


#--------------------------------------------------------------------------
# No need to change anything below here.
#--------------------------------------------------------------------------

publicFileName = 'nodes-public'
privateFileName = 'nodes-private'

if not dryRun:
    fpublic = open(publicFileName, 'w')
    fprivate = open(privateFileName, 'w')

print 'Using boto version', boto.Version
if (debug):
    boto.set_stream_logger('h2o-ec2')
ec2 = boto.ec2.connect_to_region(regionName, debug=debug)

print 'Launching', numInstancesToLaunch, 'instances.'

reservation = ec2.run_instances(
    image_id=amiId,
    min_count=numInstancesToLaunch,
    max_count=numInstancesToLaunch,
    key_name=keyName,
    instance_type=instanceType,
    security_groups=[securityGroupName],
    dry_run=dryRun
)

for i in range(numInstancesToLaunch):
    instance = reservation.instances[i]
    print 'Waiting for instance', i, '...'
    while instance.state != 'running':
        print '    .'
        time.sleep(5)
        instance.update()
    print '    instance', i, 'is up.'
    name = instanceNameRoot + str(i)
    instance.add_tag('Name', value=name)

print
print 'Creating output files: ', publicFileName, privateFileName
print

for i in range(numInstancesToLaunch):
    instance = reservation.instances[i]
    instanceName = ''
    if 'Name' in instance.tags:
        instanceName = instance.tags['Name'];
    print 'Instance', i
    print '    Name:   ', instanceName
    print '    PUBLIC: ', instance.public_dns_name
    print '    PRIVATE:', instance.private_ip_address
    print
    fpublic.write(instance.public_dns_name + '\n')
    fprivate.write(instance.private_ip_address + '\n')

fpublic.close()
fprivate.close()
