import time, sys, json, re, getpass, os, shutil
from h2o_test import get_sandbox_name, dump_json, verboseprint

#*********************************************************************************************
# duplicate do_json_request here, because the normal one is a method on a node object which 
# doesn't exist yet. copied this from find_cloud.py
def create_url(addr, port, loc):
    return 'http://%s:%s/%s' % (addr, port, loc)

#*********************************************************************************************
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
        elif r.status_code != requests.codes.ok:
            print "INFO: Could not decode any json from the request. code:" % r.status_code
            rjson = None

    except requests.ConnectionError, e:
        print "INFO: json got ConnectionError or other exception"
        rjson = None

    # print rjson
    return rjson

#*********************************************************************************************
def probe_node(line, h2oNodes, expectedSize):
    http_addr, sep, port = line.rstrip('\n').partition(":")
    http_addr = http_addr.lstrip('/') # just in case it's an old-school flatfile with leading /
    if port == '':
        port = '54321'
    if http_addr == '':
        http_addr = '127.0.0.1'
    # print "http_addr:", http_addr, "port:", port

    probes = []
    gc = do_json_request(http_addr, port, 'Cloud.json', timeout=10)
    if gc is None:
        return probes
        
    consensus  = gc['consensus']
    locked     = gc['locked']
    cloud_size = gc['cloud_size']
    node_name  = gc['node_name']
    cloud_name = gc['cloud_name']
    nodes      = gc['nodes']

    if expectedSize and (cloud_size!=expectedSize):
        raise Exception("cloud_size %s at %s disagrees with -expectedSize %s" % \
            (cloud_size, node_name, args.expectedSize))

    for n in nodes:
        # print "free_mem_bytes (GB):", "%0.2f" % ((n['free_mem_bytes']+0.0)/(1024*1024*1024))
        # print "tot_mem_bytes (GB):", "%0.2f" % ((n['tot_mem_bytes']+0.0)/(1024*1024*1024))
        java_heap_GB = (n['tot_mem_bytes']+0.0)/(1024*1024*1024)
        java_heap_GB = int(round(java_heap_GB,0))
        # print "java_heap_GB:", java_heap_GB
        # print 'num_cpus:', n['num_cpus']

        name = n['name'].lstrip('/')
        # print 'name:', name
        ### print dump_json(n)

        ip, sep, port = name.partition(':')
        # print "ip:", ip
        # print "port:", port
        if not ip or not port:
            raise Exception("bad ip or port parsing from h2o get_cloud nodes 'name' %s" % n['name'])

        # creating the list of who this guy sees, to return
        probes.append(name)

        node_id = len(h2oNodes)

        use_maprfs = 'mapr' in args.hdfs_version
        use_hdfs = not use_maprfs # we default to enabling cdh4 on 172.16.2.176
        node = { 
            'http_addr': ip, 
            'port': int(port),  # print it as a number for the clone ingest
            'java_heap_GB': java_heap_GB,
            # this list is based on what tests actually touch (fail without these)
            'node_id': node_id,
            'remoteH2O': 'true',
            'sandbox_error_was_reported': 'false', # odd this is touched..maybe see about changing h2o.py
            'sandbox_ignore_errors': 'false',
            'username': '0xcustomer', # most found clouds are run by 0xcustomer. This doesn't really matter
            'redirect_import_folder_to_s3_path': 'false', # no..we're not on ec2
            'redirect_import_folder_to_s3n_path': 'false', # no..we're not on ec2
            'delete_keys_at_teardown': 'true', # yes we want each test to clean up after itself
            'use_hdfs': use_hdfs,
            'use_maprfs': use_maprfs,
            'h2o_remote_buckets_root': 'false',
            'hdfs_version': args.hdfs_version, # something is checking for this.
            'hdfs_name_node': args.hdfs_name_node, # hmm. do we have to set this to do hdfs url generation correctly?
            'hdfs_config': args.hdfs_config,
        }

        # this is the total list so far
        if name not in h2oNodes:
            h2oNodes[name] = node
            print "Added node %s to probes" % name

    # we use this for our one level of recursion
    return probes # might be empty!

#*********************************************************************************************
# returns a json expandedCloud object that should be the same as what -ccj gets after json loads?
# also writes h2o_fc-nodes.json for debug
def find_cloud(ip_port='localhost:54321', 
    hdfs_version='cdh4', hdfs_config=None, hdfs_name_node='172.16.1.176', 
    expectedSize=1, nodesJsonPathname="h2o_fc-nodes.json"):
    # hdfs_config can be the hdfs xml config file
    # hdfs_name_node an be ip, ip:port, hostname, hostname:port", 
    # None on expected size means don't check

    # partition returns a 3-tuple as (LHS, separator, RHS) if the separator is found, 
    # (original_string, '', '') if the separator isn't found
    possMembers = [ip_port]

    h2oNodes = {}
    probes = set()
    tries = 0
    # we could just take a single node's word on the complete cloud, but this 
    # two layer try is no big deal and gives some checking robustness when a bad cloud exists
    for n1, possMember in enumerate(possMembers):
        tries += 1
        if possMember not in probes:
            probes.add(possMember)
            members2 = probe_node(possMember, h2oNodes, expectedSize)
            for n2, member2 in enumerate(members2):
                tries += 1
                if member2 not in probes:
                    probes.add(member2)
                    probe_node(member2, h2oNodes)

    print "\nWe did %s tries" % tries
    print "len(probe):", len(probes)

    # get rid of the name key we used to hash to it
    h2oNodesList = [v for k, v in h2oNodes.iteritems()]

    print "Checking for two h2os at same ip address"
    ips = {}
    count = {}
    for h in h2oNodesList:
        # warn for more than 1 h2o on the same ip address
        # error for more than 1 h2o on the same port (something is broke!)
        # but ip+port is how we name them, so that can't happen ehrer
        ip = h['http_addr']
        if ip in ips:
            # FIX! maybe make this a fail exit in the future?
            count[ip] += 1
            print "\nWARNING: appears to be %s h2o's at the same IP address" % count[ip]
            print "initial:", ips[ip]
            print "another:", h, "\n"
        else:
            ips[ip] = h
            count[ip] = 1

    print "Writing", nodesJsonPathname
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
        'h2o_nodes': h2oNodesList
        }

    # just write in current directory?
    with open(nodesJsonPathname, 'w+') as f:
        f.write(json.dumps(expandedCloud, indent=4))

    return expandedCloud

