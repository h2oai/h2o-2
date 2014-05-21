#!/usr/bin/python
import random, jenkinsapi, getpass, re, os, argparse, shutil, json, logging, sys
import string
from jenkinsapi.jenkins import Jenkins 
# only used when we wanted to see what objects were available (below)
from see import see

DO_LAST_GOOD = False

# using the env variables to force jenkinsapi to use proxy..but after to clear to avoid
# problems in other python stuff that uses requests!
def clear_env():
    # need to set environment variables for proxy server if going to sm box
    # or clear them if not!
    if os.environ.get('HTTPS_PROXY'):
        print "removing HTTPS_PROXY os env variable so requests won't use it"
        del os.environ['HTTPS_PROXY']

    if os.environ.get('HTTP_PROXY'):
        print "removing HTTP_PROXY os env variable so requests won't use it"
        del os.environ['HTTP_PROXY']
import sys


def my_hook(type, value, traceback):
    print 'hooked the exception so we can clear env variables'
    clear_env()
    print 'Type:', type
    print 'Value:', value
    print 'Traceback:', traceback
    raise Exception

sys.excepthook = my_hook

parse = argparse.ArgumentParser()
group = parse.add_mutually_exclusive_group()
group.add_argument('-e', help="job number from a list of ec2 known jobs",  type=int, action='store', default=None)
group.add_argument('-x', help="job number from a list of 164 known jobs",  type=int, action='store', default=None)
group.add_argument('-s', help="job number from a list of sm known jobs",  type=int, action='store', default=None)
group.add_argument('-j', '--jobname', help="jobname. Correct url is found",  action='store', default=None)
parse.add_argument('-l', '--logging', help="turn on logging.DEBUG msgs to see allUrls used",  action='store_true')
parse.add_argument('-v', '--verbose', help="dump the last N stdout from the failed jobs",  action='store_true')
group.add_argument('-c', help="do a hardwired special job copy between jenkins",  type=int, action='store', default=None)
args = parse.parse_args()

print "creates jsandbox (cleaned), and puts aTxt.txt and aConsole.txt in there, along with artifacts"
print " also creates fails* and regress* in there"
# can refer to this by zero-based index with -n 0  or -n 1  etc
# or by job name with -j h2o_master_test

allowedJobsX = [
    'h2o_master_test',
    'h2o_release_tests',
    'h2o_release_tests2',
    'h2o_release_tests_164',
    'h2o_release_tests_c10_only',
    'h2o_perf_test',
    'h2o_release_Runit',
]

allowedJobsE = [
    'h2o.tests.single.jvm',
    'h2o.tests.single.jvm.fvec',
    'h2o.multi.vm.temporary',
    'h2o.tests.ec2.multi.jvm',
    'h2o.tests.ec2.multi.jvm.fvec',
    'h2o.tests.ec2.hosts',
]

allowedJobsS = [
    'sm_testdir_single_jvm',
    'sm_testdir_single_jvm_fvec',
    'sm_testdir_multi_jvm',
    'sm_testdir_hosts',
    'sm_test_NN2_mnist',
]

allUrls = {
    'ec2': 'http://test.0xdata.com',
    '164': 'http://192.168.1.164:8080',
    'sm': 'http://10.71.0.163:8080',
}

all164Jobs = ['do all', 'h2o_master_test', 'h2o_master_test2', 'h2o_perf_test', 'h2o_private_json_vers_Runit', 'h2o_release_Runit', 'h2o_release_tests', 'h2o_release_tests2', 'h2o_release_tests_164', 'h2o_release_tests_c10_only', 'h2o_release_tests_cdh3', 'h2o_release_tests_cdh4', 'h2o_release_tests_cdh4_yarn', 'h2o_release_tests_cdh5', 'h2o_release_tests_cdh5_yarn', 'h2o_release_tests_hdp1.3', 'h2o_release_tests_hdp2.0.6', 'h2o_release_tests_mapr', 'selenium12']


allEc2Jobs = ['generic.h2o.build.branch', 'h2o.branch.api-dev', 'h2o.branch.cliffc-drf', 'h2o.branch.hilbert', 'h2o.branch.jobs', 'h2o.branch.jobs1', 'h2o.branch.json_versioning', 'h2o.branch.rel-ito', 'h2o.build', 'h2o.build.api-dev', 'h2o.build.gauss', 'h2o.build.godel', 'h2o.build.h2oscala', 'h2o.build.hilbert', 'h2o.build.jobs', 'h2o.build.master', 'h2o.build.rel-ito', 'h2o.build.rel-ivory', 'h2o.build.rel-iwasawa', 'h2o.build.rel-jacobi', 'h2o.build.rel-jordan', 'h2o.build.rest_api_versioning', 'h2o.build.ux-client', 'h2o.build.va_defaults_renamed', 'h2o.clone', 'h2o.datasets', 'h2o.download.latest', 'h2o.ec2.start', 'h2o.ec2.stop', 'h2o.findbugs', 'h2o.multi.vm.temporary', 'h2o.multi.vm.temporary.cliffc-no-limits', 'h2o.nightly', 'h2o.nightly.1', 'h2o.nightly.cliffc-lock', 'h2o.nightly.ec2', 'h2o.nightly.ec2.cliffc-no-limits', 'h2o.nightly.ec2.erdos', 'h2o.nightly.ec2.hilbert', 'h2o.nightly.ec2.rel-ito', 'h2o.nightly.ec2.rel-jacobi', 'h2o.nightly.ec2.rel-jordan', 'h2o.nightly.fourier', 'h2o.nightly.godel', 'h2o.nightly.multi.vm', 'h2o.nightly.rel-ivory', 'h2o.nightly.rel-iwasawa', 'h2o.nightly.rel-jacobi', 'h2o.nightly.rel-jordan', 'h2o.nightly.va_defaults_renamed', 'h2o.post.push', 'h2o.private.nightly', 'h2o.tests.ec2', 'h2o.tests.ec2.hosts', 'h2o.tests.ec2.multi.jvm', 'h2o.tests.ec2.multi.jvm.fvec', 'h2o.tests.golden', 'h2o.tests.junit', 'h2o.tests.multi.jvm', 'h2o.tests.multi.jvm.fvec', 'h2o.tests.single.jvm', 'h2o.tests.single.jvm.fvec', 'h2o.tests.test']

allSmJobs = [
    'sm_testdir_single_jvm',
    'sm_testdir_single_jvm_fvec',
    'sm_testdir_multi_jvm',
    'sm_testdir_hosts',
    'sm_test_NN2_mnist',
]


# jenkinsapi: 
# This library wraps up that interface as more 
# conventional python objects in order to make many 
# Jenkins oriented tasks easier to automate.
# http://pythonhosted.org//jenkinsapi
# https://pypi.python.org/pypi/jenkinsapi

# Project source code: github: https://github.com/salimfadhley/jenkinsapi
# Project documentation: https://jenkinsapi.readthedocs.org/en/latest/

#************************************************

if args.logging:
    logging.basicConfig(level=logging.DEBUG)

if args.jobname and (args.e or args.x or args.s):
    raise Exception("Don't use both -j and -x or -e or -s args")

# default ec2 0
jobname = None
if args.e is not None:
    if args.e<0 or args.e>(len(allowedJobsE)-1):
        raise Exception("ec2 job number %s is outside allowed range: 0-%s" % \
            (args.e, len(allowedJobsE)-1))
    jobname = allowedJobsE[args.e]

if args.x is not None:
    if args.x<0 or args.x>(len(allowedJobsX)-1):
        raise Exception("0xdata job number %s is outside allowed range: 0-%s" % \
            (args.x, len(allowedJobsX)-1))
    jobname = allowedJobsX[args.x]

if args.s is not None:
    if args.s<0 or args.s>(len(allowedJobsS)-1):
        raise Exception("sm job number %s is outside allowed range: 0-%s" % \
            (args.s, len(allowedJobsS)-1))
    jobname = allowedJobsS[args.s]

if args.jobname:
    if args.jobname not in allowedJobs:
        raise Exception("%s not in list of legal jobs" % args.jobname)
    jobname = args.jobname


if not (args.jobname or args.x or args.e or args.s):
    # prompt the user
    subtract = 0
    prefix = "-e"
    eDone = False
    xDone = False
    while not jobname: 
        allAllowedJobs = allowedJobsE + allowedJobsX + allowedJobsS
        for j, job in enumerate(allAllowedJobs):
            # first boundary
            if not eDone and j==(subtract + len(allowedJobsE)):
                subtract += len(allowedJobsE)
                prefix = "-x"
                eDone = True
            # second boundary
            if not xDone and j==(subtract + len(allowedJobsX)):
                subtract += len(allowedJobsX)
                prefix = "-s"
                xDone = True
            

            print prefix, j-subtract, " [%s]: %s" % (j, job)

        userInput = int(raw_input("Enter number (0 to %s): " % (len(allAllowedJobs)-1) ))
        if userInput >=0 and userInput <= len(allAllowedJobs):
            jobname = allAllowedJobs[userInput]

# defaults
if jobname in allEc2Jobs:
    machine = 'ec2'
elif jobname in all164Jobs:
    machine = '164'
elif jobname in allSmJobs:
    machine = 'sm'
    print "Setting up proxy server for sm"
    os.environ['HTTP_PROXY'] = 'http://172.16.0.3:8888'
    os.environ['HTTPS_PROXY'] = 'https://172.16.0.3:8888'

else:
    raise Exception("%s not in lists of known jobs" % jobname)

if machine not in allUrls:
    raise Exception("%s not in allUrls dict" % machine)
jenkins_url = allUrls[machine]

print "machine:", machine
#************************************************
def clean_sandbox(LOG_DIR="sandbox"):
    if os.path.exists(LOG_DIR):
        shutil.rmtree(LOG_DIR)
    # it should have been removed, but on error it might still be there
    if not os.path.exists(LOG_DIR):
        os.mkdir(LOG_DIR)
    return LOG_DIR

#************************************************
# get the username/pswd from files in the user's .ec2 dir (don't want cleartext here)
# prompt if doesn't exist
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
LOG_DIR = clean_sandbox("sandbox")

def dump_json(j):
    return json.dumps(j, sort_keys=True, indent=2)

#************************************************8

J = Jenkins(jenkins_url, username, password)

print "\nCurrent jobs available at %s" % jenkins_url
print J.keys()
print "\nChecking this job:", J[jobname]
job = J[jobname]
print "\nGetting %s job config" % jobname
print job.get_config
print "\nlast good build:"
lgb = job.get_last_good_build()
print "\nlast good build revision:"
print lgb.get_revision()

from jenkinsapi.api import get_latest_complete_build
from jenkinsapi.api import get_latest_test_results

# print "************************HELLO****************************"
# print get_latest_complete_build(jenkins_url, jobname, username=username, password=password)
# print "************************HELLO****************************"
# get_latest_test_results(jenkinsurl, jobname, username=None, password=None)[source]


# search_artifact_by_regexp.py
if 1==0:
    expr = "commands.log"
    print("testing search_artifact_by_regexp with expression %s") % expr
    from jenkinsapi.api import search_artifact_by_regexp
    artifact_regexp = re.compile(expr)  # A file name I want.
    result = search_artifact_by_regexp(jenkins_url, jobname, artifact_regexp)
    print("tested search_artifact_by_regexp", (repr(result)))


# print "last_stable_buildnumber", job.get_last_stable_buildnumber()
print "last_good_buildnumber", job.get_last_good_buildnumber()
# print "last_failed_buildnumber", job.get_last_failed_buildnumber()
print "last_buildnumber", job.get_last_buildnumber()


if DO_LAST_GOOD:
    print "Using last_good_buildnumber %s for result set" % job.get_last_good_buildnumber()
    build = job.get_build(job.get_last_good_buildnumber())
else:
    print "Using last_buildnumber %s for result set" % job.get_last_buildnumber()
    build = job.get_build(job.get_last_buildnumber())

af = build.get_artifacts()
dict_af = build.get_artifact_dict()


# for looking at object in json
# import h2o_util
# s = h2o_util.json_repr(dict_af, curr_depth=0, max_depth=12)
# print dump_json(s)

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
# rs.items is a generator?

#****************************************************************************
PRINTALL = False
# keep count of status counts

# 2014-03-19 07:26:15+00:00
# buildtimestampe is a datetime object
see(buildtimestamp)
t = buildtimestamp
# hour minute
hm = "%s_%s" % (t.hour, t.minute)
# hour minute second
hms = "%s_%s" % (hm, t.second)
failName = "%s_%s_%s_%s%s" % ("fail", jobname, buildnumber, hm, ".txt")
print "failName:", failName
regressName = "%s_%s_%s_%s%s" % ("regress", jobname, buildnumber, hm, ".txt")
print "regressName:", regressName
fixedName = "%s_%s_%s_%s%s" % ("fixed", jobname, buildnumber, hm, ".txt")
print "fixedName:", fixedName

stats = {}

def fprint (*args):
    # emulate printing each as string, then join with spaces
    s = ["%s" % a for a in args]
    line = " ".join(s)
    fTxt.write(line + "\n")
    print line

def printStuff():
    e1 = "\n******************************************************************************"
    e2 = "%s %s %s" % (i, jobname, v)
    fprint(e1)
    fprint(e2)
    # print "\n", k, "\n"
    # print "\n", v, "\n"
    # to see what you can get
    # print see(v)
    # print dir(v)
    # print vars(v)
    # .age .className .duration  .errorDetails .errorStackTrace .failedSince 
    # .identifier()  .name .skipped .skippedMessage  .status  .stderr .stdout
    fprint (i, "v.duration", v.duration)
    fprint (i, "v.errorStackTrace", v.errorStackTrace)
    fprint (i, "v.failedSince", v.failedSince)

    if args.verbose:
        fprint (i, "v.stderr", v.stderr)
        # lines = v.stdout.splitlines()
        # keep newlines in the list elements
        if not v.stdout:
            fprint ("v.stdout is empty")
        else:
            fprint ("len(v.stdout):", len(v.stdout))
            # have to fix the \n and \tat in the strings
            stdout = v.stdout
            # json string has the actual '\' and 'n' or 'tat' chars
            stdout = string.replace(stdout,'\\n', '\n');
            stdout = string.replace(stdout,'\\tat', '\t');
            # don't need double newlines
            stdout = string.replace(stdout,'\n\n', '\n');
            lineList = stdout.splitlines()
            fprint ("len(lineList):", len(lineList))
            num = min(20, len(lineList))
            if num!=0:
                # print i, "Last %s lineList of stdout %s" % (num, "\n".join(lineList[-num]))
                fprint (i, "Last %s lineList of stdout\n" % num)
                fprint ("\n".join(lineList[-num:]))
            else:
                fprint ("v.stdout is empty")


#******************************************************
for i, (k, v) in enumerate(rs.items()):
    if v.status in stats:
        stats[v.status] += 1
    else:
        stats[v.status] = 1

    # print rs.name
    e1 = "\n******************************************************************************"
    e2 = "%s %s %s" % (i, jobname, v)
    aTxt.write(e1+"\n")
    aTxt.write(e2+"\n")

    # only if not PASSED
    if v.status == 'FAILED':
        fTxt = open(LOG_DIR + "/" + failName, "a")
        printStuff()
        fTxt.close()

    if v.status == 'REGRESSION':
        fTxt = open(LOG_DIR + "/" + regressName, "a")
        printStuff()
        fTxt.close()

    if v.status == 'FIXED':
        fTxt = open(LOG_DIR + "/" + fixedName, "a")
        printStuff()
        fTxt.close()

    if PRINTALL:
        fprint (i, "k", k)
        fprint (i, "v", v)
        fprint (i, "v.errorDetails", v.errorDetails)
        fprint (i, "v.age", v.age)
        fprint (i, "v.className", v.className)
        fprint (i, "v.identifier()", v.identifier())
        fprint (i, "v.name", v.name)
        fprint (i, "v.skipped", v.age)
        fprint (i, "v.skippedMessage", v.skippedMessage)
        fprint (i, "v.status", v.status)
        fprint (i, "v.stdout", v.stdout)

#****************************************************************************
# print "dict_af", dict_af
if 1==1:
    for a in af:
        # print "a.keys():", a.keys()
        # txt = a.get_data()
        e = "%s %s %s %s\n" % ("#", a.filename, a.url, "########### artifact saved ####################")
        # print e,
        aTxt.write(e+"\n")

        # get the h2o output from the runit runs
        # a.save_to_dir(LOG_DIR)
        consoleTxt.close()
        # print txt
        # a.save_to_dir('./sandbox')
        # print txt[0]

aTxt.close()

print "#***********************************************"
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

# rename the sandbox
dirname = "%s_%s_%s_%s" % ("sandbox", jobname, buildnumber, hm)
if os.path.exists(dirname):
    shutil.rmtree(dirname)
os.rename(LOG_DIR, dirname)
print "Results are in", dirname

print "#***********************************************"
clear_env()

# from jenkins.py, we can copy jobs?
#     def jobs(self):
#     def get_jobs(self):
#     def get_jobs_info(self):
#     def get_job(self, jobname):
#     def has_job(self, jobname):
#     def create_job(self, jobname, config_):
#        Create a job
#        :param jobname: name of new job, str
#        :param config: configuration of new job, xml
#        :return: new Job obj
#     def copy_job(self, jobname, newjobname):

#     def build_job(self, jobname, params=None):
#        Invoke a build by job name
#        :param jobname: name of exist job, str
#        :param params: the job params, dict
#        :return: none

#     def delete_job(self, jobname):
#     def rename_job(self, jobname, newjobname):


# load config calls get_config?
# def load_config(self):
# def get_config(self):
# '''Returns the config.xml from the job'''
# def get_config_xml_url(self):
# def update_config(self, config):
# def create(self, job_name, config):
#        Create a job
#        :param jobname: name of new job, str
#        :param config: configuration of new job, xml
#        :return: new Job obj
