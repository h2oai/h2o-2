#!/usr/bin/python
import random

import jenkinsapi
import getpass
from jenkinsapi.jenkins import Jenkins 
import re, os
import argparse
import shutil
import json
import logging
logging.basicConfig(level=logging.DEBUG)
# jenkinsapi: 
# This library wraps up that interface as more 
# conventional python objects in order to make many 
# Jenkins oriented tasks easier to automate.
# http://pythonhosted.org//jenkinsapi
# https://pypi.python.org/pypi/jenkinsapi

# Project source code: github: https://github.com/salimfadhley/jenkinsapi
# Project documentation: https://jenkinsapi.readthedocs.org/en/latest/

def dump_json(j):
    return json.dumps(j, sort_keys=True, indent=2)

#************************************************8
parse = argparse.ArgumentParser()
parse.add_argument('-t', help="use 'test.0xdata.com:8080' for ip", action='store_true')
parse.add_argument('-x', help="use '192.168.1.164:8080' for ip", action='store_true')
args = parse.parse_args()

jenkins_url = 'http://192.168.1.164:8080'
jobName = 'h2o_master_test'
machine = "_164"

jenkins_url = 'http://test.0xdata.com'
jobName = 'h2o.tests.single.jvm'
machine = "ec2"

if args.x:
    jenkins_url = 'http://192.168.1.164:8080'
    jobName = 'h2o_master_test'
    machine = "164"
if args.t:
    jenkins_url = 'http://test.0xdata.com'
    jobName = 'h2o.tests.single.jvm'
    machine = "ec2"

#************************************************8
def clean_sandbox(LOG_DIR="./sandbox"):
    if os.path.exists(LOG_DIR):
        shutil.rmtree(LOG_DIR, ignore_errors=False)
    # it should have been removed, but on error it might still be there
    if not os.path.exists(LOG_DIR):
        os.mkdir(LOG_DIR)
    return LOG_DIR

#************************************************8
def login(machine='164'):
    def getit(k):
        if not os.path.isfile(k):
            print "you probably should create this file to avoid typing %s" % k
            return None
        else:
            with open(k) as f:
                lines = f.read().splitlines()
            return lines[0]

    home = os.path.expanduser("~")
    username = getit(home + '/.ec2/jenkins_user_' + machine)
    pswd = getit(home + '/.ec2/jenkins_pswd_' + machine)

    if not username:
        username = raw_input("Username [%s]: " % getpass.getuser())
    if not pswd:
        pswd = getpass.getpass()

    return username, pswd
#************************************************8
username, password = login(machine)
LOG_DIR = clean_sandbox()

#************************************************8

J = Jenkins(jenkins_url, username, password)

print J.keys()
print J[jobName]
job = J[jobName]

lgb = job.get_last_good_build()
print lgb.get_revision()

print job.get_config


# search_artifact_by_regexp.py
if 1==0:
    from jenkinsapi.api import search_artifact_by_regexp
    artifact_regexp = re.compile("commands.log")  # A file name I want.
    result = search_artifact_by_regexp(jenkins_url, jobName, artifact_regexp)
    print((repr(result)))


# print "last_stable_buildnumber", job.get_last_stable_buildnumber()
print "last_good_buildnumber", job.get_last_good_buildnumber()
# print "last_failed_buildnumber", job.get_last_failed_buildnumber()
print "last_buildnumber", job.get_last_buildnumber()

build = job.get_build(job.get_last_buildnumber())

af = build.get_artifacts()
dict_af = build.get_artifact_dict()
print "build get_status", build.get_status()
print "build name", build.name
print "build number", build.get_number()
print "build revision", build.get_revision()
print "build revision branch", build.get_revision_branch()
print "build duration", build.get_duration()
print "build upstream job name", build.get_upstream_job_name()
print "build is_good", build.is_good()

print "build timestamp", build.get_timestamp()
consoleTxt = open(LOG_DIR + '/console.txt', "a")
print "getting build console (how to buffer this write?)"
print "probably better to figure how to save it as file"
c = build.get_console()
consoleTxt.write(c)
consoleTxt.close()

print "build has result set", build.has_resultset()
print "build get result set"
rs = build.get_resultset()
print "build result set name", rs.name
print "build result set items", rs.items()
print #****************************************
# print dump_json(item)
print "build result set keys", rs.keys()
aTxt = open(LOG_DIR + '/artifacts.txt', "a")
for k, v in rs.items():
    e = "%s %s %s\n" % (rs.name, k, v)
    print e,
    aTxt.write(e)
    print "\n", rs[k], "\n"

print "dict_af", dict_af
for a in af:
    # print "a.keys():", a.keys()
    # txt = a.get_data()
    e = "%s %s %s %s\n" % ("#", a.filename, a.url, "####################################")
    print e,
    aTxt.write(e+"\n")

    # get the h2o output from the runit runs
    if 'java_' in a.filename:
        a.save_to_dir(LOG_DIR)
    consoleTxt.close()
    # print txt
    # a.save_to_dir('./sandbox')
    # print txt[0]

aTxt.close()
