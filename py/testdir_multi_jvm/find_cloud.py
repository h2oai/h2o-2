import unittest, time, sys, json, re
sys.path.extend(['.','..','py'])

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
for l in lines:
    print l, l[0], l[2]
        

print "Starting Trial", trial
print "Just doing node[%s]" % NODE
getCloudFirst = None
    # we just want the string
    start = time.time()
    getCloud = n.get_cloud()
    elapsed = int(1000 * (time.time() - start)) # milliseconds
    print "get_cloud completes to node", i, "in", "%s"  % elapsed, "millisecs"
    getCloudString = json.dumps(getCloud)

    print h2o.dump_json(getCloud)
    h2o.verboseprint(json.dumps(getCloud,indent=2))

    def get_cloud(self, timeoutSecs=10):
        # hardwire it to allow a 60 second timeout
        a = self.__do_json_request('Cloud.json', timeout=timeoutSecs)

        consensus  = a['consensus']
        locked     = a['locked']
        cloud_size = a['cloud_size']
        cloud_name = a['cloud_name']
        node_name  = a['node_name']
        node_id    = self.node_id
        verboseprint('%s%s %s%s %s%s %s%s' %(
            "\tnode_id: ", node_id,
            "\tcloud_size: ", cloud_size,
            "\tconsensus: ", consensus,
            "\tlocked: ", locked,
            ))
        return a

  def __do_json_request(self, jsonRequest=None, fullUrl=None, timeout=10, params=None,
        cmd='get', extraComment=None, ignoreH2oError=False, **kwargs):
        # if url param is used, use it as full url. otherwise crate from the jsonRequest
        if fullUrl:
            url = fullUrl
        else:
            url = self.__url(jsonRequest)

        # remove any params that are 'None'
        # need to copy dictionary, since can't delete while iterating
        if params is not None:
            params2 = params.copy()
            for k in params2:
                if params2[k] is None:
                    del params[k]
            paramsStr =  '?' + '&'.join(['%s=%s' % (k,v) for (k,v) in params.items()])
        else:
            paramsStr = ''

        if extraComment:
            log('Start ' + url + paramsStr, comment=extraComment)
        else:
            log('Start ' + url + paramsStr)

        # file get passed thru kwargs here
        if cmd=='post':
            r = requests.post(url, timeout=timeout, params=params, **kwargs)
        else:
            r = requests.get(url, timeout=timeout, params=params, **kwargs)

        # fatal if no response
        if not beta_features and not r:
            raise Exception("Maybe bad url? no r in __do_json_request in %s:" % inspect.stack()[1][3])

        # this is used to open a browser on results, or to redo the operation in the browser
        # we don't' have that may urls flying around, so let's keep them all
        json_url_history.append(r.url)
        if not beta_features and not r.json():
            raise Exception("Maybe bad url? no r.json in __do_json_request in %s:" % inspect.stack()[1][3])

        rjson = None

        try:
            rjson = r.json()
        except:
            print(r.text)
            if not isinstance(r,(list,dict)):
                raise Exception("h2o json responses should always be lists or dicts, see previous for text")
            if '404' in r:
                raise Exception("json got 404 response. Do you have beta features turned on? beta_features: ", beta_features)
            raise Exception("Could not decode any json from the request. Do you have beta features turned on? beta_features: ", beta_features)





        for e in ['error', 'Error', 'errors', 'Errors']:
            if e in rjson:
                verboseprint(dump_json(rjson))
                emsg = 'rjson %s in %s: %s' % (e, inspect.stack()[1][3], rjson[e])
                if ignoreH2oError:
                    # well, we print it..so not totally ignore. test can look at rjson returned
                    print emsg
                else:
                    raise Exception(emsg)

        for w in ['warning', 'Warning', 'warnings', 'Warnings']:
            if w in rjson:
                verboseprint(dump_json(rjson))
                print 'rjson %s in %s: %s' % (w, inspect.stack()[1][3], rjson[w])

        return rjson




if __name__ == '__main__':
    h2o.unit_main()
