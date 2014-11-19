#!/usr/bin/python
import time, sys, json, re, getpass, requests, argparse, os, shutil

parser = argparse.ArgumentParser(description='Creates h2o-node.json for cloud cloning from existing cloud')

# parser.add_argument('-v', '--verbose',     help="verbose", action='store_true')
parser.add_argument('-f', '--flatfile', 
    help="Use this flatfile to start probes\ndefaults to pytest_flatfile-<username> which is created by python tests", 
    type=str)

parser.add_argument('-hdfs_version', '--hdfs_version', 
    choices=['0.20.2', 'cdh4', 'cdh4', 'cdh4_yarn', 'cdh5', 'mapr2.1.3', 'mapr3.0.1', 'mapr3.1.1', 'hdp2.1'],
    default='cdh4', 
    help="Use this for setting hdfs_version in the cloned cloud", 
    type=str)

parser.add_argument('-hdfs_config', '--hdfs_config', 
    default=None,
    help="Use this for setting hdfs_config in the cloned cloud", 
    type=str)

parser.add_argument('-hdfs_name_node', '--hdfs_name_node', 
    default='172.16.2.176',
    help="Use this for setting hdfs_name_node in the cloned cloud. Can be ip, ip:port, hostname, hostname:port", 
    type=str)

parser.add_argument('-expected_size', '--expected_size', 
    default=None,
    help="Require that the discovered cloud has this size, at each discovered node, otherwise exception",
    type=int)

args = parser.parse_args()

#            "hdfs_version": "cdh4", 
#            "hdfs_config": "None", 
#            "hdfs_name_node": "172.16.2.176", 
#********************************************************************
# shutil.rmtree doesn't work on windows if the files are read only.
# On unix the parent dir has to not be readonly too.
# May still be issues with owner being different, like if 'system' is the guy running?
# Apparently this escape function on errors is the way shutil.rmtree can
# handle the permission issue. (do chmod here)
# But we shouldn't have read-only files. So don't try to handle that case.
def handleRemoveError(func, path, exc):
    # If there was an error, it could be due to windows holding onto files.
    # Wait a bit before retrying. Ignore errors on the retry. Just leave files.
    # Ex. if we're in the looping cloud test deleting sandbox.
    excvalue = exc[1]
    print "Retrying shutil.rmtree of sandbox (2 sec delay). Will ignore errors. Exception was", excvalue.errno
    time.sleep(2)
    try:
        func(path)
    except OSError:
        pass

def get_sandbox_name():
    if os.environ.has_key("H2O_SANDBOX_NAME"):
        a = os.environ["H2O_SANDBOX_NAME"]
        print "H2O_SANDBOX_NAME", a
        return a
    else:
        return "sandbox"

LOG_DIR = get_sandbox_name()

# Create a clean sandbox, like the normal cloud builds...because tests
# expect it to exist (they write to sandbox/commands.log)
# find_cloud.py creates h2o-node.json for tests to use with -ccj
# the side-effect they also want is clean sandbox, so we'll just do it here too
def clean_sandbox():
    if os.path.exists(LOG_DIR):
        # shutil.rmtree fails to delete very long filenames on Windoze
        #shutil.rmtree(LOG_DIR)
        # was this on 3/5/13. This seems reliable on windows+cygwin
        ### os.system("rm -rf "+LOG_DIR)
        shutil.rmtree(LOG_DIR, ignore_errors=False, onerror=handleRemoveError)
    # it should have been removed, but on error it might still be there
    if not os.path.exists(LOG_DIR):
        os.mkdir(LOG_DIR)

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
        # may get legitimate 404
        # elif '404' in r.text:
        #     print "INFO: json got 404 result"
        #    rjson = None
        elif r.status_code != requests.codes.ok:
            print "INFO: Could not decode any json from the request. code:" % r.status_code
            rjson = None

    except requests.ConnectionError, e:
        print "INFO: json got ConnectionError or other exception"
        rjson = None

    # print rjson
    return rjson

#********************************************************************

# we create this node level state that h2o_import.py uses to create urls
# normally it's set during build_cloud. 
# As a hack, we're going to force it, from arguments to find_cloud
# everything should just work then...the runner*sh know what hdfs clusters they're targeting
# so can tell us (and they built the cloud anyhow!..so they are the central place that's deciding
# hdfs_config is only used on ec2, but we'll support it in case find_cloud.py is used there.
#            "hdfs_version": "cdh4", 
#            "hdfs_config": "None", 
#            "hdfs_name_node": "172.16.2.176", 
# force these to the right state, although I should update h2o*py stuff, so they're not necessary
# they force the prior settings to be ignore (we used to do stuff if hdfs was enabled, writing to hdfs
# this was necessary to override the settings above that caused that to happen
#            "use_hdfs": true, 
#            "use_maprfs": false, 

def probe_node(line, h2oNodes):
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

    if args.expected_size and (cloud_size!=args.expected_size):
        raise Exception("cloud_size %s at %s disagrees with -expected_size %s" % (cloud_size, node_name, args.expected_size))

    for n in nodes:
        print "free_mem_bytes (GB):", "%0.2f" % ((n['free_mem_bytes']+0.0)/(1024*1024*1024))
        print "max_mem_bytes (GB):", "%0.2f" % ((n['max_mem_bytes']+0.0)/(1024*1024*1024))
        java_heap_GB = (n['max_mem_bytes']+0.0)/(1024*1024*1024)
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
            # /home/0xcustomer will have the superset of links for resolving remote paths
            # the cloud may be started by 0xdiag or 0xcustomer, but this is just going to be
            # used for looking for buckets (h2o_import.find_folder_and_filename() will 
            # (along with other rules) try to look in # /home/h2o.nodes[0].username when trying 
            # to resolve a path to a bucket
            'username': '0xcustomer', # most found clouds are run by 0xcustomer. This doesn't really matter
            'redirect_import_folder_to_s3_path': 'false', # no..we're not on ec2
            'redirect_import_folder_to_s3n_path': 'false', # no..we're not on ec2
            'delete_keys_at_teardown': 'true', # yes we want each test to clean up after itself
            'use_hdfs': use_hdfs,
            'use_maprfs': use_maprfs,
            'h2o_remote_buckets_root': 'false',
            'hdfs_version': args.hdfs_version, # something is checking for this. I guess we could set this in tests as a hack
            'hdfs_name_node': args.hdfs_name_node, # hmm. do we have to set this to do hdfs url generation correctly?
            'hdfs_config': args.hdfs_config,
            'aws_credentials': 'false',
        }

        # this is the total list so far
        if name not in h2oNodes:
            h2oNodes[name] = node
            print "Added node %s to probes" % name

    # we use this for our one level of recursion
    return probes # might be empty!

#********************************************************************
def flatfile_pathname():
    if args.flatfile:
        a = args.flatfile
    else:
        print "New: match h2o.py in getting it by default from LOG_DIR (sandbox) if not specified."
        a = LOG_DIR + '/pytest_flatfile-%s' %getpass.getuser()
    print "Starting with contents of ", a
    return a

#********************************************************************
# hostPortList.append("/" + h.addr + ":" + str(port + ports_per_node*i))
# partition returns a 3-tuple as (LHS, separator, RHS) if the separator is found, 
# (original_string, '', '') if the separator isn't found
with open(flatfile_pathname(), 'r') as f:
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

        
print "Writing h2o-nodes.json"
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

print "Cleaning sandbox, (creating it), so tests can write to commands.log normally"
clean_sandbox()

with open('h2o-nodes.json', 'w+') as f:
    f.write(json.dumps(expandedCloud, indent=4))

