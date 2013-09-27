import unittest, time, sys, json, re

def dump_json(j):
    return json.dumps(j, sort_keys=True, indent=2)

def create_url(http_addr, port, loc)
    return 'http://%s:%d/%s' % (http_addr, port, loc)

def do_json_request(jsonRequest=None, params=None, timeout=10, **kwargs):
    url = create_url(jsonRequest)
    if params is not None:
        paramsStr =  '?' + '&'.join(['%s=%s' % (k,v) for (k,v) in params.items()])
    else:
        paramsStr = ''

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

def get_cloud(addr, port, timeoutSecs=10):
    a = do_json_request('Cloud.json', timeout=timeoutSecs)
    consensus  = a['consensus']
    locked     = a['locked']
    cloud_size = a['cloud_size']
    cloud_name = a['cloud_name']
    node_name  = a['node_name']
    node_id    = self.node_id
    print '%s%s %s%s %s%s %s%s' % (
        "\tnode_id: ", node_id,
        "\tcloud_size: ", cloud_size,
        "\tconsensus: ", consensus,
        "\tlocked: ", locked,
        )
    return a

def flatfile_name():
    return('pytest_flatfile-%s' %getpass.getuser())

# hostPortList.append("/" + h.addr + ":" + str(base_port + ports_per_node*i))
# partition returns a 3-tuple as (LHS, separator, RHS) if the separator is found, 
# (original_string, '', '') if the separator isn't found
with open(flatfile_name(), 'r') as f:
    lines = f.read().partition(':')
f.close()

# FIX! is there any leading "/" in the flatfile anymore?
# maybe remove it from h2o.py generation
foundCloud = {}
for n, line in enumerate(n,lines):
    
    print line, line[0], line[2]
    http_addr = line[0] 
    port = line[2]

    if port = ''
        port = '54321'
    if http_addr = ''
        http_addr = '127.0.0.1'

    node = { 'http_addr': http_addr, 'base_port': port }
    foundCloud.add(node)
    print "Added node %s %s", (n, node)

    # we just want the string
    start = time.time()
    getCloud = get_cloud(addr, port)
    elapsed = int(1000 * (time.time() - start)) # milliseconds
    print "get_cloud completes to node", i, "in", "%s"  % elapsed, "millisecs"
    print dump_json(getCloud)
    getCloudString = json.dumps(getCloud)

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
            'h2o_nodes': foundCloud
        }

    print "Writing h2o-nodes.json"
    with open('h2o-nodes.json', 'w+') as f:
        f.write(json.dumps(expandedCloud, indent=4))


if __name__ == '__main__':
    h2o.unit_main()
