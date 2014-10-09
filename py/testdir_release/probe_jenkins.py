#!/usr/bin/python
from urllib2 import urlopen
import json, re
from json import loads


def print_json(j):
    j = json.dumps(j, sort_keys=True, indent=2)
    # j = re.sub(j, r'\n', '\\n')
    print j


def my_urlopen(url):
    print "\nurlopen:", url
    return urlopen(url)

print "\nget all projects"
req = urlopen('http://172.16.2.164:8080/api/json')
res = req.read()
data = loads(res)
print_json(data.keys())
print_json(data['jobs'][0].keys())

for i,job in enumerate(data['jobs']):
    print "\njob", i
    print_json(job['name'])
    if job['name']=='h2o_release_tests':
        jobIndex = i


print "\nfull url to job", jobIndex
req = my_urlopen('%s%s' % (data['jobs'][jobIndex]['url'], 'api/json'))
res = req.read()
job = loads(res)


print_json(job.keys())
# [ "url", "number" ]

print "I think this job is h2o_release_tests: ", job['name']

print "\nwhen did", job['name'], "last run to success?"
print "job['lastCompletedBuild']:"
print_json(job['lastCompletedBuild'])
print_json(job['lastCompletedBuild'].keys())

# first part has the trailing / already
req = my_urlopen('%s%s' % (job['lastCompletedBuild']['url'], 'testReport/api/json'))
res = req.read()
testReport = loads(res)
# [ "suites", "failCount", "skipCount", "duration", "passCount", "empty" ]

# printed = 0
print testReport.keys()
printed = 0
for i in testReport['suites']:
    print "#######################################################"
    noKeysList = []


    if isinstance(i, dict) and printed<8:
        print "i.keys", i.keys()
        # i.keys [u'name', u'stdout', u'timestamp', u'stderr', u'duration', u'cases', u'id']
        # print_json(i)
        printed += 1



# {
#     "type":"object", "properties":{
#         "age": { "type":"number", },
#         "className": { "type":"string", },
#         "duration": { "type":"number", },
#         "errorDetails": { "type":"string", },
#         "errorStackTrace": { "type":"string", },
#         "failedSince": { "type":"number", },
#         "name": { "type":"string", },
#         "skippedMessage": { "type":"string", },
#         "skipped": { "type":"boolean", },
#         "status": { "type":"string", },
#         "stderr": { "type":"string", },
#         "stdout": { "type":"string", }
#     }
# }
