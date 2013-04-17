import getpass, json, h2o
import random, os
# UPDATE: all multi-machine testing will pass list of IP and base port addresses to H2O
# means we won't realy on h2o self-discovery of cluster

def find_config(base):
    f = base
    if not os.path.exists(f): f = 'testdir_hosts/' + base
    if not os.path.exists(f): f = 'py/testdir_hosts/' + base
    if not os.path.exists(f):
        raise Exception("unable to find config %s" % base)
    return f

# node_count is sometimes used positionally...break that out. all others are keyword args
def build_cloud_with_hosts(node_count=None, **kwargs):
    # legacy: we allow node_count to be positional. 
    # if it's used positionally, stick in in kwargs (overwrite if there too)
    if node_count is not None:
        # we use h2o_per_host in the config file. will translate to node_count for build_cloud
        kwargs['h2o_per_host'] = node_count
        # set node_count to None to make sure we don't use it below. 'h2o_per_host' should be used
        node_count = None

    # randomizing default base_port used
    offset = random.randint(0,31)
    # for new params:
    # Just update this list with the param name and default and you're done
    allParamsDefault = {
        'use_flatfile': None,
        'use_hdfs': None,
        'hdfs_name_node': None, 
        'hdfs_config': None,
        'hdfs_version': None,
        'base_port': None,
        'java_heap_GB': None,
        'java_heap_MB': None,
        'java_extra_args': None,
        'sigar': False,

        'timeoutSecs': 60, 
        'retryDelaySecs': 2, 
        'cleanup': True,
        'slow_connection': False,

        'h2o_per_host': 2,
        'ip':'["127.0.0.1"]', # this is for creating the hosts list
        'base_port': 54300 + offset,
        'username':'0xdiag',
        'password': None,
        'rand_shuffle': True,

        'use_home_for_ice': False,
        'key_filename': None,
        'aws_credentials': None,
        'redirect_import_folder_to_s3_path': None,
        'disable_h2o_log': False,
        'enable_benchmark_log': False,
    }
    # initialize the default values
    paramsToUse = {}
    for k,v in allParamsDefault.iteritems():
        paramsToUse[k] = allParamsDefault.setdefault(k, v)

    # allow user to specify the config json at the command line. config_json is a global.
    if h2o.config_json:
        configFilename = find_config(h2o.config_json)
    else:
        # configs may be in the testdir_hosts
        configFilename = find_config(h2o.default_hosts_file())

    h2o.verboseprint("Loading host config from", configFilename)
    with open(configFilename, 'rb') as fp:
         hostDict = json.load(fp)

    for k,v in hostDict.iteritems():
        # Don't take in params that we don't have in the list above
        # Because michal has extra params in here for ec2! and comments!
        if k in paramsToUse:
            paramsToUse[k] = hostDict.setdefault(k, v)

    # Now overwrite with anything passed by the test
    # whatever the test passes, always overrules the config json
    for k,v in kwargs.iteritems():
        paramsToUse[k] = kwargs.setdefault(k, v)

    h2o.verboseprint("All build_cloud_with_hosts params:", paramsToUse)

    #********************
    global hosts
    # Update: special case paramsToUse['ip'] = ["127.0.0.1"] and use the normal build_cloud
    # this allows all the tests in testdir_host to be run with a special config that points to 127.0.0.1
    # hosts should be None for everyone if normal build_cloud is desired
    if paramsToUse['ip']== ["127.0.0.1"]:
        hosts = None
    else:
        h2o.verboseprint("About to RemoteHost, likely bad ip if hangs")
        hosts = []
        for h in paramsToUse['ip']:
            h2o.verboseprint("Connecting to:", h)
            hosts.append(h2o.RemoteHost(addr=h, 
                username=paramsToUse['username'], 
                password=paramsToUse['password'], 
                key_filename=paramsToUse['key_filename']))

    # done with these, don't pass to build_cloud
    paramsToUse.pop('ip') # this was the list of ip's from the config file, replaced by 'hosts' to build_cloud
    paramsToUse.pop('username')
    paramsToUse.pop('password')
    paramsToUse.pop('key_filename')
   
    # handles hosts=None correctly
    h2o.write_flatfile(
        node_count=paramsToUse['h2o_per_host'],
        base_port=paramsToUse['base_port'],
        hosts=hosts,
        rand_shuffle=paramsToUse['rand_shuffle']
        )

    if hosts is not None:
        # this uploads the flatfile too
        h2o.upload_jar_to_remote_hosts(hosts, slow_connection=paramsToUse['slow_connection'])
        # timeout wants to be larger for large numbers of hosts * h2oPerHost
        # use 60 sec min, 5 sec per node.
        timeoutSecs = max(60, 8*(len(hosts) * paramsToUse['h2o_per_host']))
    else: # for 127.0.0.1 case
        timeoutSecs = 60
    paramsToUse.pop('slow_connection')

    # sandbox gets cleaned in build_cloud
    # legacy param issue
    node_count = paramsToUse['h2o_per_host']
    paramsToUse.pop('h2o_per_host')
    h2o.build_cloud(node_count, hosts=hosts, **paramsToUse)
