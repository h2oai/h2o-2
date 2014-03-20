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
# logging.basicConfig(level=logging.DEBUG)

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

#************************************************
parse = argparse.ArgumentParser()
parse.add_argument('-t', help="use 'test.0xdata.com:8080' for ip", action='store_true')
parse.add_argument('-x', help="use '192.168.1.164:8080' for ip", action='store_true')
args = parse.parse_args()

jenkins_url = 'http://192.168.1.164:8080'
jobName = 'h2o_master_test'
machine = "164"


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


print "Using last_buildnumber %s for result set" % job.get_last_buildnumber()
build = job.get_build(job.get_last_good_buildnumber())

af = build.get_artifacts()
dict_af = build.get_artifact_dict()

buildstatus = build.get_status()
print "build get_status", buildstatus

buildname = build.name
print "build name", buildname

buildnumber = build.get_number()
print "build number", buildnumber

buildrevision = build.get_revision()
print "build revision", buildrevision

buildbranch = build.get_revision_branch()
print "build revision branch", buildbranch

buildduration = build.get_duration()
print "build duration", buildduration

buildupstream = build.get_upstream_job_name()
print "build upstream job name", buildupstream

buildgood = build.is_good()
print "build is_good", buildgood

buildtimestamp = build.get_timestamp()
print "build timestamp", buildtimestamp

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
# print "build result set items", rs.items()
print #****************************************
# print dump_json(item)
# print "build result set keys", rs.keys()
aTxt = open(LOG_DIR + '/artifacts.txt', "a")

# have just a json string in the result set?
from see import see
# rs.items is a generator?

PRINTALL = False
# keep count of status counts
stats = {}
for i, (k, v) in enumerate(rs.items()):
    if v.status in stats:
        stats[v.status] += 1
    else:
        stats[v.status] = 1

    # print rs.name
    e1 = "\n******************************************************************************"
    e2 = "%s %s" % (i, v)
    aTxt.write(e1+"\n")
    aTxt.write(e2+"\n")
    # only if not PASSED
    if v.status != 'PASSED':
        print e1
        print e2
        # print e
        # print "\n", v, "\n"
        # print i, "#2***********************************"
        # print v
        # print i, "#3***********************************"
        # print "\n", k, "\n"
        # print i, "#4***********************************"
        # print "\n", v, "\n"
        # print i, "#5***********************************"
        # print see(v)
        # .age .className .duration  .errorDetails .errorStackTrace .failedSince .identifier()  .name .skipped .skippedMessage  .status  .stderr .stdout
        print i, "v.duration", v.duration
        print i, "v.errorStackTrace", v.errorStackTrace
        print i, "v.failedSince", v.failedSince
        print i, "v.stderr", v.stderr
        # lines = v.stdout.splitlines()
        lines = v.stdout
        num = min(5, len(lines))
        if num!=0:
            # print i, "Last %s lines of stdout %s" % (num, "\n".join(lines[-num]))
            print i, "Last %s lines of stdout %s" % (num, "\n", lines[-num])
        else:
            print "v.stdout is empty"


        if PRINTALL:
            print i, "k", k
            print i, "v", v
            print i, "v.errorDetails", v.errorDetails
            print i, "v.age", v.age
            print i, "v.className", v.className
            print i, "v.identifier()", v.identifier()
            print i, "v.name", v.name
            print i, "v.skipped", v.age
            print i, "v.skippedMessage", v.skippedMessage
            print i, "v.status", v.status
            print i, "v.stdout", v.stdout

        # print i, "#6***********************************"
        # print dir(v)
        # print i, "#7***********************************"
        # print vars(v)

# 'errorDetails': None, 'age': 0, 'className': 'runit', 'errorStackTrace': None, 'skippedMessage': None, 'duration': 161.5247, 'stderr': '\n\nspawn stderr2014-03-18 23:56:52.393819**********************************************************\n\n            

# print "dict_af", dict_af
if 1==1:
    for a in af:
        # print "a.keys():", a.keys()
        # txt = a.get_data()
        e = "%s %s %s %s\n" % ("#", a.filename, a.url, "########### artifact saved ####################")
        print e,
        aTxt.write(e+"\n")

        # get the h2o output from the runit runs
        a.save_to_dir(LOG_DIR)
        consoleTxt.close()
        # print txt
        # a.save_to_dir('./sandbox')
        # print txt[0]

aTxt.close()

print ""
print #***********************************************
print #***********************************************
print #***********************************************
print "Build:", buildname
print buildtimestamp
print "Status:", buildstatus
if buildgood:
    print "Build is good"
else:
    print "Build is bad"
print "Build number", buildnumber
# print buildrevision
print buildbranch
print "Duration", buildduration
print "Upstream job", buildupstream
print "Test summary"
for s in stats:
    print s, stats[s]
print #***********************************************
