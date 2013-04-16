#!/usr/bin/env python

# Usage
#send_to_s3.py "<s3_path>" "<local_file_path>"

import argparse
import time
from os.path import expanduser

home = expanduser("~")

parser = argparse.ArgumentParser()
parser.add_argument('s3_path', help="path to use inside s3 bucket 'home-0xdiag-datasets'")
parser.add_argument('local_file_path', help="path to local file")
args = parser.parse_args()

import sys
import os

# should just be one line, but you never know
with open(home + '/.ec2/aws_id') as f:
    lines = f.read().splitlines()
aws_id = lines[0]
print "  Using aws_id:", aws_id

with open(home + '/.ec2/aws_key') as f:
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

if 1==1:
    # show everything in the buckei
    rs = pb.list()
    for key in rs:
       print key.name

# Make an S3 key using the bucket
k = Key(pb)
file_name_to_use_in_s3 = "%s/%s"%(args.s3_path, os.path.basename(args.local_file_path))
print "Going to get", file_name_to_use_in_s3

# Set the name of the file to use in S3
# S3 doesn't have the concept of directories
# Use / in the file name to mimic the directory path
k.name = file_name_to_use_in_s3
# Get the file from S3
start = time.time()
k.get_contents_to_filename(args.local_file_path)
elapsed = time.time() - start
local_file_size = os.path.getsize(args.local_file_path)
print "  Got %s to %s"%(file_name_to_use_in_s3, args.local_file_path)
print "  Filesize was", local_file_size, "bytes"
print "  Speed was", "%6.2f" % (local_file_size / (1e6 * elapsed)), "MB/Sec"




