#!/usr/bin/python
import random

import jenkinsapi
import getpass
from jenkinsapi.jenkins import Jenkins 
import re
# jenkinsapi: 
# This library wraps up that interface as more 
# conventional python objects in order to make many 
# Jenkins oriented tasks easier to automate.
# http://pythonhosted.org//jenkinsapi
# https://pypi.python.org/pypi/jenkinsapi

# Project source code: github: https://github.com/salimfadhley/jenkinsapi
# Project documentation: https://jenkinsapi.readthedocs.org/en/latest/


# ['do all', 'h2o_master_test', 'h2o_master_test2', 'h2o_perf_test', 'h2o_private_json_vers_Runit', 'h2o_release_Runit', 'h2o_release_tests', 'h2o_release_tests2', 'h2o_release_tests_164', 'h2o_release_tests_c10_only', 'h2o_release_tests_cdh3', 'h2o_release_tests_cdh4', 'h2o_release_tests_cdh4_yarn', 'h2o_release_tests_cdh5', 'h2o_release_tests_cdh5_yarn', 'h2o_release_tests_hdp1.3', 'h2o_release_tests_hdp2.0.6', 'h2o_release_tests_mapr', 'selenium12']



def login(username='none'):
    user = raw_input("Username [%s]: " % username)
    if not username:
        user = getpass.getuser()
    p1 = getpass.getpass()
    return user, p1

username, password = login()
jenkinsurl = 'http://192.168.1.164:8080'
jobName = 'h2o_master_test'
J = Jenkins(jenkinsurl, username, password)

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
    result = search_artifact_by_regexp(jenkinsurl, jobName, artifact_regexp)
    print((repr(result)))


print "last_stable_buildnumber", job.get_last_stable_buildnumber()
print "last_good_buildnumber", job.get_last_good_buildnumber()
print "last_failed_buildnumber", job.get_last_failed_buildnumber()
print "last_buildnumber", job.get_last_buildnumber()

build = job.get_build(job.get_last_good_buildnumber())

print "build revision", build.get_revision()
af = build.get_artifacts()
dict_af = build.get_artifact_dict()
print "build get_status", build.get_status()
print "build name", build.name
print "build number", build.get_number()

print "dict_af", dict_af

for a in af:
    print "a.keys():", a.keys()
    txt = a.get_data()
    print "####################################"
    # print txt
