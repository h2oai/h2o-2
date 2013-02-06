
# used to create example of what we want for the json file
# not used for normal execution

hostDict = {}
hostDict['username'] = '0xdiag'
hostDict['password'] = '0xdiag'
hostDict['h2o_per_host'] = 2
hostDict['ip'] = []
hostDict['ip'].append('192.168.0.30')
hostDict['ip'].append('192.168.0.31')
hostDict['ip'].append('192.168.0.32')
hostDict['ip'].append('192.168.0.33')
hostDict['ip'].append('192.168.0.34')

jsonConfig = json.dumps(hostDict, sort_keys=False, indent=4)
print jsonConfig
with open('example.json', 'wb') as fp:
    json.dump(hostDict, fp, sort_keys=False, indent=4)

