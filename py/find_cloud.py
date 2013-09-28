#!/usr/bin/python
import time, sys, json, re, getpass, requests

def dump_json(j):
    return json.dumps(j, sort_keys=True, indent=2)

def create_url(addr, port, loc):
    return 'http://%s:%s/%s' % (addr, port, loc)

def do_json_request(addr=None, port=None,  jsonRequest=None, params=None, timeout=10, **kwargs):
    if params is not None:
        paramsStr =  '?' + '&'.join(['%s=%s' % (k,v) for (k,v) in params.items()])
    else:
        paramsStr = ''

    url = create_url(addr, port, jsonRequest)
    print 'Start ' + url + paramsStr
    r = requests.get(url, timeout=timeout, params=params, **kwargs)

    try:
        rjson = r.json()
    except:
        print(r.text)
        if not isinstance(r,(list,dict)):
            raise Exception("h2o json responses should always be lists or dicts")
        if '404' in r:
            raise Exception("json got 404 result")
        raise Exception("Could not decode any json from the request")

    return rjson

def get_cloud(addr, port,  timeoutSecs=10):
    return do_json_request(addr, port, 'Cloud.json', timeout=timeoutSecs)

# print lines
# FIX! is there any leading "/" in the flatfile anymore?
# maybe remove it from h2o.py generation

#********************************************************************
def probe_node(line):
    http_addr, sep, port = line.rstrip('\n').partition(":")
    print "http_addr:", http_addr, "port:", port

    if port == '':
        port = '54321'
    if http_addr == '':
        http_addr = '127.0.0.1'

    # we just want the string
    start = time.time()
    gc = get_cloud(http_addr, port)
    consensus  = gc['consensus']
    locked     = gc['locked']
    cloud_size = gc['cloud_size']
    cloud_name = gc['cloud_name']
    node_name  = gc['node_name']
    nodes      = gc['nodes']

    probes = []
    for n in nodes:
        print "free_mem_bytes (GB):", "%0.2f" % ((n['free_mem_bytes']+0.0)/(1024*1024*1024))
        print "tot_mem_bytes (GB):", "%0.2f" % ((n['tot_mem_bytes']+0.0)/(1024*1024*1024))
        java_heap_GB = (n['tot_mem_bytes']+0.0)/(1024*1024*1024)
        java_heap_GB = round(java_heap_GB,2)
        print "java_heap_GB:", java_heap_GB

        print 'name:', n['name'].lstrip('/')
        print 'num_cpus:', n['num_cpus']
        ### print dump_json(n)
        ip, sep, port = n['name'].lstrip('/').partition(':')
        print "ip:", ip
        print "port:", port
        if not ip or not port:
            raise Exception("bad ip or port parsing from h2o get_cloud nodes 'name' %s" % n['name'])

        # we'll just overwrite dictionary entries..assume overwrites match..can check!
        # maybe don't overwrite!
        newName = ip + ':' + port
        probes.append(newName)

    gcString = json.dumps(gc)

    # FIX! walk thru all the ips in the result

    node = { 'http_addr': http_addr, 'base_port': port }
    
    # FIX! search the list we currently have
    h2oNodes.append(node)
    print "Added node %s %s" % (n, node)

    # we use this for our one level of recursion
    return probes

#********************************************************************
def flatfile_name():
    return('pytest_flatfile-%s' %getpass.getuser())

# hostPortList.append("/" + h.addr + ":" + str(base_port + ports_per_node*i))
# partition returns a 3-tuple as (LHS, separator, RHS) if the separator is found, 
# (original_string, '', '') if the separator isn't found
with open(flatfile_name(), 'r') as f:
    lines1 = f.readlines()
f.close()

h2oNodes = []
probes = set()
tries = 0
for n1, line1 in enumerate(lines1):
    tries += 1
    if line1 not in probes:
        probes.add(line1)
        lines2 = probe_node(line1)
    for n2, line2 in enumerate(lines2):
        tries += 1
        if line2 not in probes:
            probes.add(line2)
            probe_node(line2)

print "\nWe did %s tries" % tries, "n1:", n1, "n2", n2
print "len(probe):", len(probes)

expandedCloud = {
    'cloud_start':
        {
        'time': 'null',
        'cwd': 'null',
        'python_test_name': 'null',
        'python_cmd_line': 'null',
        'config_json': 'null',
        'username': 'null',
        'ip': 'null',
        },
    'h2oNodes': h2oNodes
    }

print "Writing h2o-nodes.json"
with open('h2o-nodes.json', 'w+') as f:
    f.write(json.dumps(expandedCloud, indent=4))

