#!/usr/bin/python


import os
import sys
import time
import boto
import boto.ec2
import argparse


# Define arguments
parser = argparse.ArgumentParser(description='H2o Cloud Launch.')
parser.add_argument('-n','--number',help='Number of Instances',type=int,required=True)
parser.add_argument('-c','--cloudname',help='Name of Cloud',required=True)
args = parser.parse_args()

# Print Output
print ("Cloud size: %s" % args.number )
print ("Cloud name: %s" % args.cloudname )
# Environment variables you MUST set (either here or by passing them in).
# -----------------------------------------------------------------------
#
#os.environ['AWS_ACCESS_KEY_ID'] = '...'
#os.environ['AWS_SECRET_ACCESS_KEY'] = '...'
#os.environ['AWS_SSH_PRIVATE_KEY_FILE'] = '...'



# Launch EC2 instances with an IAM role
# --------------------------------------
# 
iam_profile_resource_name = None
# or
iam_profile_name = None

# Options you MUST tailor to your own AWS account.
# ------------------------------------------------

# SSH key pair name.
keyName = '...' 

# AWS security group name.
# Note:
#     H2O uses TCP and UDP ports 54321 and 54322.
#     RStudio uses TCP port 8787.
securityGroupName = 'SecurityDisabled'


# Options you might want to change.
# ---------------------------------

numInstancesToLaunch = args.number
instanceType = 'm1.large'
instanceNameRoot = args.cloudname


# Options to help debugging.
# --------------------------

debug = 0
# debug = 1
dryRun = False
# dryRun = True


# Options you should not change unless you really mean to.
# --------------------------------------------------------

regionName = 'us-east-1'
amiId = 'ami-c0df5ea8'


#--------------------------------------------------------------------------
# No need to change anything below here.
#--------------------------------------------------------------------------

# Note: this python script was initially developed with boto 2.13.3.
def botoVersionMismatch():
    print 'WARNING:  Unsupported boto version.  Please upgrade boto to at least 2.13.x and try again.'
    print 'Comment this out to run anyway.'
    print 'Exiting.'
    sys.exit(1)

if not 'AWS_ACCESS_KEY_ID' in os.environ:
    print 'ERROR: You must set AWS_ACCESS_KEY_ID in the environment.'
    sys.exit(1)

if not 'AWS_SECRET_ACCESS_KEY' in os.environ:
    print 'ERROR: You must set AWS_SECRET_ACCESS_KEY in the environment.'
    sys.exit(1)

if not 'AWS_SSH_PRIVATE_KEY_FILE' in os.environ:
    print 'ERROR: You must set AWS_SSH_PRIVATE_KEY_FILE in the environment.'
    sys.exit(1)

publicFileName = '/usr/share/nginx/html/temp/nodes-public'
privateFileName = '/usr/share/nginx/html/temp/nodes-private'

if not dryRun:
    fpublic = open(publicFileName, 'w')
    fprivate = open(privateFileName, 'w')

print 'Using boto version', boto.Version
if True:
    botoVersionArr = boto.Version.split(".")
    if (botoVersionArr[0] != 2):
        botoVersionMismatch
    if (botoVersionArr[1] < 13):
        botoVersionMismatch

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
    instance_profile_arn=iam_profile_resource_name,
    instance_profile_name=iam_profile_name,
    dry_run=dryRun
)

for i in range(numInstancesToLaunch):
    instance = reservation.instances[i]
    print 'Waiting for instance', i+1, 'of', numInstancesToLaunch, '...'
    instance.update()
    while instance.state != 'running':
        print '    .'
        time.sleep(1)
        instance.update()
    print '    instance', i+1, 'of', numInstancesToLaunch, 'is up.'
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
    print 'Instance', i+1, 'of', numInstancesToLaunch
    print '    Name:   ', instanceName
    print '    PUBLIC: ', instance.public_dns_name
    print '    PRIVATE:', instance.private_ip_address
    print
    fpublic.write(instance.public_dns_name + '\n')
    fprivate.write(instance.private_ip_address + '\n')

fpublic.close()
fprivate.close()

print 'Creating S3 Buckets'

boto.set_stream_logger('boto')
s3 = boto.connect_s3()
bucketName = instanceNameRoot

print 'Creating:', bucketName
bucket = s3.create_bucket(bucketName)
key = bucket.new_key('nodes-public')
key.set_contents_from_filename(publicFileName)
key = bucket.new_key('nodes-private')
key.set_contents_from_filename(privateFileName)
time.sleep(2)

print 'Complete'

