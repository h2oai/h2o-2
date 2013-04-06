#!/usr/bin/env python

copies = 100
print "Will send", copies, "copies of a file to s3_path in 'home-0xdiag-datasets' bucket with numerical suffix"

import argparse
parser = argparse.ArgumentParser()
parser.add_argument('local_file_path', help="path to local file to be copied to s3")
parser.add_argument('s3_path', help="path to use inside s3 bucket 'home-0xdiag-datasets'")
args = parser.parse_args()

local_file_path = args.local_file_path
s3_path = args.s3_path

import sys
import os

# should just be one line, but you never know
with open('/home/hduser/.ec2/aws_id') as f:
    lines = f.read().splitlines()
aws_id = lines[0]
print "  Using aws_id:", aws_id

with open('/home/hduser/.ec2/aws_key') as f:
    lines = f.read().splitlines()
aws_key = lines[0]
print "  Using aws_key:", aws_key

from boto.s3.connection import S3Connection
from boto.s3.key import Key

# Initiate a S3 connection using key and secret
conn = S3Connection(aws_id, aws_key)

# The bucket name
bucket = 'home-0xdiag-datasets'
pb = conn.get_bucket(bucket)
# Make an S3 key using the bucket
k = Key(pb)
k.set_metadata('Cache-Control', 'no-store, no-cache, must-revalidate, post-check=0, pre-check=0')

# Set the name of the file to use in S3
# S3 doesn't have the concept of directories
# Use / in the file name to mimic the directory path
base_file_name_to_use_in_s3 = "%s/%s"%(s3_path, os.path.basename(local_file_path))
for n in range(copies):
    file_suffix = "%05d" % n
    # add suffix to the name in s3
    k.name = base_file_name_to_use_in_s3 + "_" + file_suffix
    # Send the file to S3
    k.set_contents_from_filename(local_file_path)
    print "  Sent %s to %s in bucket: %s"%(local_file_path, k.name, bucket)
