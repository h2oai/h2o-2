from urllib2 import urlopen
import json
from json import loads


def print_json(j):
    print(json.dumps(j, sort_keys=True, indent=2))


print "\nget all projects"
req = urlopen('http://192.168.1.164:8080/api/json')
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
req = urlopen('%s/%s' % (data['jobs'][jobIndex]['url'], 'api/json'))
res = req.read()
job = loads(res)
print_json(job.keys())
print "I think this job is h2o_release_tests: ", job['name']

print "\nwhen did", job['name'], "last run to success?"
print_json(job['lastCompletedBuild'].keys())

req = urlopen('%s/%s' % (job['lastCompletedBuild']['url'], 'api/json'))
res = req.read()
build = loads(res)

print_json(build.keys())
print "\nnumber:",
print_json(build['number'])
print "\nresult:",
print_json(build['result'])
print "\ntimestamp:",
print_json(build['timestamp'])
print "\nartifacts:",
print_json(build['artifacts'])
print "\nestimatedDuration:",
print_json(build['estimatedDuration'])
# print "%0.2f minutes" % ((build['estimatedDuration'] + 0.0)/(1000 * 60 * 60))



