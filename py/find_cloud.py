#!/usr/bin/python
import time, sys, json, re, getpass, requests

def dump_json(j):
    return json.dumps(j, sort_keys=True, indent=2)

def create_url(addr, port, loc):
    return 'http://%s:%s/%s' % (addr, port, loc)

def do_json_request(addr=None, port=None,  jsonRequest=None, params=None, timeout=5, **kwargs):
    if params is not None:
        paramsStr =  '?' + '&'.join(['%s=%s' % (k,v) for (k,v) in params.items()])
    else:
        paramsStr = ''

    url = create_url(addr, port, jsonRequest)
    print 'Start ' + url + paramsStr
    try:
        r = requests.get(url, timeout=timeout, params=params, **kwargs)
        # the requests json decoder might fail if we didn't get something good
        rjson = r.json()
        if not isinstance(rjson, (list,dict)):
            # probably good
            print "INFO: h2o json responses should always be lists or dicts"
            rjson = None
        elif '404' in r.text:
            print "INFO: json got 404 result"
            rjson = None
        elif r.status_code != requests.codes.ok:
            print "INFO: Could not decode any json from the request. code:" % r.status_code
            rjson = None

    except requests.ConnectionError, e:
        print "INFO: json got ConnectionError or other exception"
        rjson = None

    print rjson
    return rjson

#********************************************************************
def probe_node(line, h2oNodes):
    http_addr, sep, port = line.rstrip('\n').partition(":")
    if port == '':
        port = '54321'
    if http_addr == '':
        http_addr = '127.0.0.1'
    print "http_addr:", http_addr, "port:", port

    probes = []
    gc = do_json_request(http_addr, port, 'Cloud.json', timeout=3)
    if gc is None:
        return probes
        
    consensus  = gc['consensus']
    locked     = gc['locked']
    cloud_size = gc['cloud_size']
    cloud_name = gc['cloud_name']
    node_name  = gc['node_name']
    nodes      = gc['nodes']

    for n in nodes:
        print "free_mem_bytes (GB):", "%0.2f" % ((n['free_mem_bytes']+0.0)/(1024*1024*1024))
        print "tot_mem_bytes (GB):", "%0.2f" % ((n['tot_mem_bytes']+0.0)/(1024*1024*1024))
        java_heap_GB = (n['tot_mem_bytes']+0.0)/(1024*1024*1024)
        java_heap_GB = round(java_heap_GB,2)
        print "java_heap_GB:", java_heap_GB
        print 'num_cpus:', n['num_cpus']

        name = n['name'].lstrip('/')
        print 'name:', name
        ### print dump_json(n)

        ip, sep, port = name.partition(':')
        print "ip:", ip
        print "port:", port
        if not ip or not port:
            raise Exception("bad ip or port parsing from h2o get_cloud nodes 'name' %s" % n['name'])

        # creating the list of who this guy sees, to return
        probes.append(name)

        node = { 
            'http_addr': http_addr, 
            'base_port': port, 
            'java_heap_GB': java_heap_GB 
        }

        # this is the total list so far
        if name not in h2oNodes:
            h2oNodes[name] = node
            print "Added node %s to probes" % name

    # we use this for our one level of recursion
    return probes # might be empty!

#********************************************************************
def flatfile_name():
    a = 'pytest_flatfile-%s' %getpass.getuser()
    print "Starting with contents of ", a
    return a

# hostPortList.append("/" + h.addr + ":" + str(base_port + ports_per_node*i))
# partition returns a 3-tuple as (LHS, separator, RHS) if the separator is found, 
# (original_string, '', '') if the separator isn't found
with open(flatfile_name(), 'r') as f:
    possMembers = f.readlines()
f.close()

h2oNodes = {}
probes = set()
tries = 0
for n1, possMember in enumerate(possMembers):
    tries += 1
    if possMember not in probes:
        probes.add(possMember)
        members2 = probe_node(possMember, h2oNodes)
        for n2, member2 in enumerate(members2):
            tries += 1
            if member2 not in probes:
                probes.add(member2)
                probe_node(member2, h2oNodes)

print "\nWe did %s tries" % tries
print "len(probe):", len(probes)

# get rid of the name key we used to hash to it
h2oNodesList = [v for k, v in h2oNodes.iteritems()]
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
    'h2oNodes': h2oNodesList
    }

print "Writing h2o-nodes.json"
with open('h2o-nodes.json', 'w+') as f:
    f.write(json.dumps(expandedCloud, indent=4))

