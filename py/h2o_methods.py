
import os, sys, time, requests, zipfile, StringIO
import h2o_args
# from h2o_cmd import runInspect, infoFromSummary
import h2o_cmd, h2o_util
import h2o_browse as h2b
import h2o_print as h2p

from h2o_objects import H2O
from h2o_test import verboseprint, dump_json, check_sandbox_for_errors, get_sandbox_name, log

# print "h2o_methods"

def check_params_update_kwargs(params_dict, kw, function, print_params):
    # only update params_dict..don't add
    # throw away anything else as it should come from the model (propagating what RF used)
    for k in kw:
        if k in params_dict:
            params_dict[k] = kw[k]
        else:
            raise Exception("illegal parameter '%s' in %s" % (k, function))

    if print_params:
        print "%s parameters:" % function, params_dict
        sys.stdout.flush()


def get_cloud(self, noExtraErrorCheck=False, timeoutSecs=10):
    # hardwire it to allow a 60 second timeout
    a = self.do_json_request('Cloud.json', noExtraErrorCheck=noExtraErrorCheck, timeout=timeoutSecs)
    version    = a['version']
    if version and version!='(unknown)' and version!='null' and version!='none':
        if not version.startswith('2'):
            h2p.red_print("h2o version at node[0] doesn't look like h2o version. (start with 2) %s" % version)

    consensus = a['consensus']
    locked = a['locked']
    cloud_size = a['cloud_size']
    cloud_name = a['cloud_name']
    node_name = a['node_name']
    node_id = self.node_id
    verboseprint('%s%s %s%s %s%s %s%s %s%s' % (
        "\tnode_id: ", node_id,
        "\tcloud_size: ", cloud_size,
        "\tconsensus: ", consensus,
        "\tlocked: ", locked,
        "\tversion: ", version,
    ))
    return a

def h2o_log_msg(self, message=None, timeoutSecs=15):
    if 1 == 0:
        return
    if not message:
        message = "\n"
        message += "\n#***********************"
        message += "\npython_test_name: " + h2o_args.python_test_name
        message += "\n#***********************"
    params = {'message': message}
    self.do_json_request('2/LogAndEcho', params=params, timeout=timeoutSecs)

def get_timeline(self):
    return self.do_json_request('Timeline.json')

# Shutdown url is like a reset button. Doesn't send a response before it kills stuff
# safer if random things are wedged, rather than requiring response
# so request library might retry and get exception. allow that.
def shutdown_all(self):
    try:
        self.do_json_request('Shutdown.json', noExtraErrorCheck=True)
    except:
        pass
    # don't want delayes between sending these to each node
    # if you care, wait after you send them to each node
    # Seems like it's not so good to just send to one node
    # time.sleep(1) # a little delay needed?
    return (True)

def put_value(self, value, key=None, repl=None):
    return self.do_json_request(
        'PutValue.json',
        params={"value": value, "key": key, "replication_factor": repl},
        extraComment=str(value) + "," + str(key) + "," + str(repl))

# {"Request2":0,"response_info":i
# {"h2o":"pytest-kevin-4530","node":"/192.168.0.37:54321","time":0,"status":"done","redirect_url":null},
# "levels":[null,null,null,null]}
# FIX! what is this for? R uses it. Get one per col? maybe something about enums
def levels(self, source=None):
    return self.do_json_request(
        '2/Levels2.json',
        params={"source": source},
    )

def export_files(self, print_params=True, timeoutSecs=60, **kwargs):
    params_dict = {
        'src_key': None,
        'path': None,
        'force': None,
    }
    check_params_update_kwargs(params_dict, kwargs, 'export_files', print_params)
    return self.do_json_request(
        '2/ExportFiles.json',
        timeout=timeoutSecs,
        params=params_dict,
    )

def put_file(self, f, key=None, timeoutSecs=60):
    if key is None:
        key = os.path.basename(f)
        ### print "putfile specifying this key:", key

    fileObj = open(f, 'rb')
    resp = self.do_json_request(
        '2/PostFile.json',
        cmd='post',
        timeout=timeoutSecs,
        params={"key": key},
        files={"file": fileObj},
        extraComment=str(f))

    verboseprint("\nput_file response: ", dump_json(resp))
    fileObj.close()
    return key

# noise is a 2-tuple ("StoreView", none) for url plus args for doing during poll to create noise
# so we can create noise with different urls!, and different parms to that url
# no noise if None
def poll_url(self, response,
             timeoutSecs=10, retryDelaySecs=0.5, initialDelaySecs=0, pollTimeoutSecs=180,
             noise=None, benchmarkLogging=None, noPoll=False, reuseFirstPollUrl=False, noPrint=False):
    verboseprint('poll_url input: response:', dump_json(response))
    ### print "poll_url: pollTimeoutSecs", pollTimeoutSecs
    ### print "at top of poll_url, timeoutSecs: ", timeoutSecs

    # for the rev 2 stuff..the job_key, destination_key and redirect_url are just in the response
    # look for 'response'..if not there, assume the rev 2

    def get_redirect_url(response):
        url = None
        params = None
        # StoreView has old style, while beta_features
        if 'response_info' in response: 
            response_info = response['response_info']

            if 'redirect_url' not in response_info:
                raise Exception("Response during polling must have 'redirect_url'\n%s" % dump_json(response))

            if response_info['status'] != 'done':
                redirect_url = response_info['redirect_url']
                if redirect_url:
                    url = self.url(redirect_url)
                    params = None
                else:
                    if response_info['status'] != 'done':
                        raise Exception(
                            "'redirect_url' during polling is null but status!='done': \n%s" % dump_json(response))
        else:
            if 'response' not in response:
                raise Exception("'response' not in response.\n%s" % dump_json(response))

            if response['response']['status'] != 'done':
                if 'redirect_request' not in response['response']:
                    raise Exception("'redirect_request' not in response. \n%s" % dump_json(response))

                url = self.url(response['response']['redirect_request'])
                params = response['response']['redirect_request_args']

        return (url, params)

    # if we never poll
    msgUsed = None

    if 'response_info' in response: # trigger v2 for GBM always?
        status = response['response_info']['status']
        progress = response.get('progress', "")
    else:
        r = response['response']
        status = r['status']
        progress = r.get('progress', "")

    doFirstPoll = status != 'done'
    (url, params) = get_redirect_url(response)
    # no need to recreate the string for messaging, in the loop..
    if params:
        paramsStr = '&'.join(['%s=%s' % (k, v) for (k, v) in params.items()])
    else:
        paramsStr = ''

    # FIX! don't do JStack noise for tests that ask for it. JStack seems to have problems
    noise_enable = noise and noise != ("JStack", None)
    if noise_enable:
        print "Using noise during poll_url:", noise
        # noise_json should be like "Storeview"
        (noise_json, noiseParams) = noise
        noiseUrl = self.url(noise_json + ".json")
        if noiseParams is None:
            noiseParamsStr = ""
        else:
            noiseParamsStr = '&'.join(['%s=%s' % (k, v) for (k, v) in noiseParams.items()])

    start = time.time()
    count = 0
    if initialDelaySecs:
        time.sleep(initialDelaySecs)

    # can end with status = 'redirect' or 'done'
    # Update: on DRF2, the first RF redirects to progress. So we should follow that, and follow any redirect to view?
    # so for v2, we'll always follow redirects?
    # For v1, we're not forcing the first status to be 'poll' now..so it could be redirect or done?(NN score? if blocking)

    # Don't follow the Parse redirect to Inspect, because we want parseResult['destination_key'] to be the end.
    # note this doesn't affect polling with Inspect? (since it doesn't redirect ?
    while status == 'poll' or doFirstPoll or (status == 'redirect' and 'Inspect' not in url):
        count += 1
        if ((time.time() - start) > timeoutSecs):
            # show what we're polling with
            emsg = "Exceeded timeoutSecs: %d secs while polling." % timeoutSecs + \
                   "status: %s, url: %s?%s" % (status, urlUsed, paramsUsedStr)
            raise Exception(emsg)

        if benchmarkLogging:
            import h2o
            h2o.cloudPerfH2O.get_log_save(benchmarkLogging)

        # every other one?
        create_noise = noise_enable and ((count % 2) == 0)
        if create_noise:
            urlUsed = noiseUrl
            paramsUsed = noiseParams
            paramsUsedStr = noiseParamsStr
            msgUsed = "\nNoise during polling with"
        else:
            urlUsed = url
            paramsUsed = params
            paramsUsedStr = paramsStr
            msgUsed = "\nPolling with"

        print status, progress, urlUsed
        time.sleep(retryDelaySecs)

        response = self.do_json_request(fullUrl=urlUsed, timeout=pollTimeoutSecs, params=paramsUsed)
        verboseprint(msgUsed, urlUsed, paramsUsedStr, "Response:", dump_json(response))
        # hey, check the sandbox if we've been waiting a long time...rather than wait for timeout
        if ((count % 6) == 0):
            check_sandbox_for_errors(python_test_name=h2o_args.python_test_name)

        if (create_noise):
            # this guarantees the loop is done, so we don't need to worry about
            # a 'return r' being interpreted from a noise response
            status = 'poll'
            progress = ''
        else:
            doFirstPoll = False
            status = response['response_info']['status']
            progress = response.get('progress', "")
            # get the redirect url
            if not reuseFirstPollUrl: # reuse url for all v1 stuff
                (url, params) = get_redirect_url(response)

            if noPoll:
                return response

    # won't print if we didn't poll
    if msgUsed:
        verboseprint(msgUsed, urlUsed, paramsUsedStr, "Response:", dump_json(response))
    return response

# this is only for 2 (fvec)
def kmeans_view(self, model=None, timeoutSecs=30, **kwargs):
    # defaults
    params_dict = {
        '_modelKey': model,
    }
    browseAlso = kwargs.get('browseAlso', False)
    # only lets these params thru
    check_params_update_kwargs(params_dict, kwargs, 'kmeans_view', print_params=True)
    print "\nKMeans2ModelView params list:", params_dict
    a = self.do_json_request('2/KMeans2ModelView.json', timeout=timeoutSecs, params=params_dict)

    # kmeans_score doesn't need polling?
    verboseprint("\nKMeans2Model View result:", dump_json(a))

    if (browseAlso | h2o_args.browse_json):
        print "Redoing the KMeans2ModelView through the browser, no results saved though"
        h2b.browseJsonHistoryAsUrlLastMatch('KMeans2ModelView')
        time.sleep(5)
    return a

# additional params include: cols=.
# don't need to include in params_dict it doesn't need a default
# FIX! cols should be renamed in test for fvec
def kmeans(self, key, key2=None,
    timeoutSecs=300, retryDelaySecs=0.2, initialDelaySecs=None, pollTimeoutSecs=180,
    noise=None, benchmarkLogging=None, noPoll=False, **kwargs):
    # defaults
    # KMeans has more params than shown here
    # KMeans2 has these params?
    # max_iter=100&max_iter2=1&iterations=0
    params_dict = {
        'initialization': 'Furthest',
        'k': 1,
        'source': key,
        'destination_key': key2,
        'seed': None,
        'cols': None,
        'ignored_cols': None,
        'ignored_cols_by_name': None,
        'max_iter': None,
        'normalize': None,
        'drop_na_cols': None,
    }

    if key2 is not None: params_dict['destination_key'] = key2
    browseAlso = kwargs.get('browseAlso', False)
    # only lets these params thru
    check_params_update_kwargs(params_dict, kwargs, 'kmeans', print_params=True)
    algo = '2/KMeans2'

    print "\n%s params list:" % algo, params_dict
    a1 = self.do_json_request(algo + '.json',
        timeout=timeoutSecs, params=params_dict)

    if noPoll:
        return a1

    a1 = self.poll_url(a1, timeoutSecs=timeoutSecs, retryDelaySecs=retryDelaySecs,
        initialDelaySecs=initialDelaySecs, pollTimeoutSecs=pollTimeoutSecs,
        noise=noise, benchmarkLogging=benchmarkLogging)
    print "For now, always dumping the last polled kmeans result ..are the centers good"
    print "\n%s result:" % algo, dump_json(a1)

    # if we want to return the model view like the browser
    if 1==0:
        # HACK! always do a model view. kmeans last result isn't good? (at least not always)
        a = self.kmeans_view(model=a1['model']['_key'], timeoutSecs=30)
        verboseprint("\n%s model view result:" % algo, dump_json(a))
    else:
        a = a1

    if (browseAlso | h2o_args.browse_json):
        print "Redoing the %s through the browser, no results saved though" % algo
        h2b.browseJsonHistoryAsUrlLastMatch(algo)
        time.sleep(5)
    return a

# params:
# header=1,
# header_from_file
# separator=1 (hex encode?
# exclude=
# noise is a 2-tuple: ("StoreView",params_dict)

def parse(self, key, key2=None,
          timeoutSecs=300, retryDelaySecs=0.2, initialDelaySecs=None, pollTimeoutSecs=180,
          noise=None, benchmarkLogging=None, noPoll=False, **kwargs):
    browseAlso = kwargs.pop('browseAlso', False)
    # this doesn't work. webforums indicate max_retries might be 0 already? (as of 3 months ago)
    # requests.defaults({max_retries : 4})
    # https://github.com/kennethreitz/requests/issues/719
    # it was closed saying Requests doesn't do retries. (documentation implies otherwise)
    algo = "2/Parse2"
    verboseprint("\n %s key: %s to key2: %s (if None, means default)" % (algo, key, key2))
    # other h2o parse parameters, not in the defauls
    # header
    # exclude
    params_dict = {
        'blocking': None, # debug only
        'source_key': key, # can be a regex
        'destination_key': key2,
        'parser_type': None,
        'separator': None,
        'header': None,
        'single_quotes': None,
        'header_from_file': None,
        'exclude': None,
        'delete_on_done': None,
        'preview': None,
    }
    check_params_update_kwargs(params_dict, kwargs, 'parse', print_params=True)

    # h2o requires header=1 if header_from_file is used. Force it here to avoid bad test issues
    if kwargs.get('header_from_file'): # default None
        kwargs['header'] = 1

    if benchmarkLogging:
        import h2o
        h2o.cloudPerfH2O.get_log_save(initOnly=True)

    a = self.do_json_request(algo + ".json", timeout=timeoutSecs, params=params_dict)

    # Check that the response has the right Progress url it's going to steer us to.
    verboseprint(algo + " result:", dump_json(a))

    if noPoll:
        return a

    # noise is a 2-tuple ("StoreView, none) for url plus args for doing during poll to create noise
    # no noise if None
    verboseprint(algo + ' noise:', noise)
    a = self.poll_url(a, timeoutSecs=timeoutSecs, retryDelaySecs=retryDelaySecs,
                      initialDelaySecs=initialDelaySecs, pollTimeoutSecs=pollTimeoutSecs,
                      noise=noise, benchmarkLogging=benchmarkLogging)

    verboseprint("\n" + algo + " result:", dump_json(a))
    return a

def netstat(self):
    return self.do_json_request('Network.json')

def linux_info(self, timeoutSecs=30):
    return self.do_json_request("CollectLinuxInfo.json", timeout=timeoutSecs)

def jstack(self, timeoutSecs=30):
    return self.do_json_request("JStack.json", timeout=timeoutSecs)

def network_test(self, tdepth=5, timeoutSecs=30):
    a = self.do_json_request("2/NetworkTest.json", params={}, timeout=timeoutSecs)
    verboseprint("\n network test:", dump_json(a))
    return(a)

def jprofile(self, depth=5, timeoutSecs=30):
    return self.do_json_request("2/JProfile.json", params={'depth': depth}, timeout=timeoutSecs)

def iostatus(self):
    return self.do_json_request("IOStatus.json")


# turns enums into expanded binary features
def one_hot(self, source, timeoutSecs=30, **kwargs):
    params = {
        "source": source,
    }

    a = self.do_json_request('2/OneHot.json',
                               params=params,
                               timeout=timeoutSecs
    )

    check_sandbox_for_errors(python_test_name=h2o_args.python_test_name)
    return a

# &offset=
# &view=
# FIX! need to have max > 1000? 
def inspect(self, key, offset=None, view=None, max_column_display=1000, ignoreH2oError=False,
            timeoutSecs=30):
    params = {
        "src_key": key,
        "offset": offset,
        # view doesn't exist for 2. let it be passed here from old tests but not used
    }
    a = self.do_json_request('2/Inspect2.json',
        params=params,
        ignoreH2oError=ignoreH2oError,
        timeout=timeoutSecs
    )
    return a

# can take a useful 'filter'
# FIX! current hack to h2o to make sure we get "all" rather than just
# default 20 the browser gets. set to max # by default (1024)
# There is a offset= param that's useful also, and filter=
def store_view(self, timeoutSecs=60, print_params=False, **kwargs):
    params_dict = {
        # now we should default to a big number, so we see everything
        'filter': None,
        'view': 10000,
        'offset': 0,
    }
    # no checking on legal kwargs?
    params_dict.update(kwargs)
    if print_params:
        print "\nStoreView params list:", params_dict

    a = self.do_json_request('StoreView.json',
                               params=params_dict,
                               timeout=timeoutSecs)
    return a

def rebalance(self, timeoutSecs=180, **kwargs):
    params_dict = {
        # now we should default to a big number, so we see everything
        'source': None,
        'after': None,
        'chunks': None,
    }
    params_dict.update(kwargs)
    a = self.do_json_request('2/ReBalance.json',
                               params=params_dict,
                               timeout=timeoutSecs
    )
    verboseprint("\n rebalance result:", dump_json(a))
    return a

def to_int(self, timeoutSecs=60, **kwargs):
    params_dict = {
        'src_key': None,
        'column_index': None, # ugh. takes 1 based indexing
    }
    params_dict.update(kwargs)
    a = self.do_json_request('2/ToInt2.json', params=params_dict, timeout=timeoutSecs)
    verboseprint("\n to_int result:", dump_json(a))
    return a

def to_enum(self, timeoutSecs=60, **kwargs):
    params_dict = {
        'src_key': None,
        'column_index': None, # ugh. takes 1 based indexing
    }
    params_dict.update(kwargs)
    a = self.do_json_request('2/ToEnum2.json', params=params_dict, timeout=timeoutSecs)
    verboseprint("\n to_int result:", dump_json(a))
    return a

def unlock(self, timeoutSecs=30):
    a = self.do_json_request('2/UnlockKeys.json', params=None, timeout=timeoutSecs)
    return a

# There is also a RemoveAck in the browser, that asks for confirmation from
# the user. This is after that confirmation.
# UPDATE: ignore errors on remove..key might already be gone due to h2o removing it now
# after parse
def remove_key(self, key, timeoutSecs=120):
    a = self.do_json_request('Remove.json',
        params={"key": key}, ignoreH2oError=True, timeout=timeoutSecs)
    self.unlock()
    return a


# this removes all keys!
def remove_all_keys(self, timeoutSecs=120):
    a = self.do_json_request('2/RemoveAll.json', timeout=timeoutSecs)
    return a

# only model keys can be exported?
def export_hdfs(self, source_key, path):
    a = self.do_json_request('ExportHdfs.json',
                               params={"source_key": source_key, "path": path})
    verboseprint("\nexport_hdfs result:", dump_json(a))
    return a

def export_s3(self, source_key, bucket, obj):
    a = self.do_json_request('ExportS3.json',
                               params={"source_key": source_key, "bucket": bucket, "object": obj})
    verboseprint("\nexport_s3 result:", dump_json(a))
    return a

# the param name for ImportFiles is 'file', but it can take a directory or a file.
# 192.168.0.37:54323/ImportFiles.html?file=%2Fhome%2F0xdiag%2Fdatasets
def import_files(self, path, timeoutSecs=180):
    a = self.do_json_request('2/ImportFiles2.json',
        timeout=timeoutSecs,
        params={"path": path}
    )
    verboseprint("\nimport_files result:", dump_json(a))
    return a

# 'destination_key', 'escape_nan' 'expression'
def exec_query(self, timeoutSecs=20, ignoreH2oError=False, print_params=False, **kwargs):
    # only v2 now
    params_dict = {
        'str': None,
    }

    browseAlso = kwargs.pop('browseAlso', False)
    check_params_update_kwargs(params_dict, kwargs, 'exec_query', print_params=print_params)
    a = self.do_json_request('2/Exec2.json',
        timeout=timeoutSecs, ignoreH2oError=ignoreH2oError, params=params_dict)
    verboseprint("\nexec_query result:", dump_json(a))
    return a

def jobs_admin(self, timeoutSecs=120, **kwargs):
    params_dict = {
        # 'expression': None,
    }
    browseAlso = kwargs.pop('browseAlso', False)
    params_dict.update(kwargs)
    verboseprint("\njobs_admin:", params_dict)
    a = self.do_json_request('Jobs.json', timeout=timeoutSecs, params=params_dict)
    verboseprint("\njobs_admin result:", dump_json(a))
    return a

def jobs_cancel(self, timeoutSecs=120, **kwargs):
    params_dict = {
        'key': None,
    }
    browseAlso = kwargs.pop('browseAlso', False)
    check_params_update_kwargs(params_dict, kwargs, 'jobs_cancel', print_params=True)
    a = self.do_json_request('Cancel.json', timeout=timeoutSecs, params=params_dict)
    verboseprint("\njobs_cancel result:", dump_json(a))
    print "Cancelled job:", params_dict['key']

    return a

def create_frame(self, timeoutSecs=120, **kwargs):
    params_dict = {
        'key': None,
        'rows': None,
        'cols': None,
        'seed': None,
        'randomize': None,
        'value': None,
        'real_range': None,
        'binary_fraction': None,
        'categorical_fraction': None,
        'factors': None,
        'integer_fraction': None,
        'integer_range': None,
        'binary_fraction': None,
        'binary_ones_fraction': None,
        'missing_fraction': None,
        'response_factors': None,
        'has_response': None,
    }
    browseAlso = kwargs.pop('browseAlso', False)
    check_params_update_kwargs(params_dict, kwargs, 'create_frame', print_params=True)
    a = self.do_json_request('2/CreateFrame.json', timeout=timeoutSecs, params=params_dict)
    verboseprint("\ncreate_frame result:", dump_json(a))
    return a

def insert_missing_values(self, timeoutSecs=120, **kwargs):
    params_dict = {
        'key': None,
        'seed': None,
        'missing_fraction': None,
    }
    browseAlso = kwargs.pop('browseAlso', False)
    check_params_update_kwargs(params_dict, kwargs, 'insert_missing_values', print_params=True)
    a = self.do_json_request('2/InsertMissingValues.json', timeout=timeoutSecs, params=params_dict)
    verboseprint("\ninsert_missing_values result:", dump_json(a))
    return a

def impute(self, timeoutSecs=120, **kwargs):
    params_dict = {
        'source': None,
        'column': None,
        'method': None, # mean, mode, median
        'group_by': None, # comma separated column names
    }
    browseAlso = kwargs.pop('browseAlso', False)
    check_params_update_kwargs(params_dict, kwargs, 'impute', print_params=True)
    a = self.do_json_request('2/Impute.json', timeout=timeoutSecs, params=params_dict)
    verboseprint("\nimpute result:", dump_json(a))
    return a

def frame_split(self, timeoutSecs=120, **kwargs):
    params_dict = {
        'source': None,
        'ratios': None,
    }
    browseAlso = kwargs.pop('browseAlso', False)
    check_params_update_kwargs(params_dict, kwargs, 'frame_split', print_params=True)
    a = self.do_json_request('2/FrameSplitPage.json', timeout=timeoutSecs, params=params_dict)
    verboseprint("\nframe_split result:", dump_json(a))
    return a

def nfold_frame_extract(self, timeoutSecs=120, **kwargs):
    params_dict = {
        'source': None,
        'nfolds': None,
        'afold': None, # Split to extract
    }
    browseAlso = kwargs.pop('browseAlso', False)
    check_params_update_kwargs(params_dict, kwargs, 'nfold_frame_extract', print_params=True)
    a = self.do_json_request('2/NFoldFrameExtractPage.json', timeout=timeoutSecs, params=params_dict)
    verboseprint("\nnfold_frame_extract result:", dump_json(a))
    return a

def gap_statistic(self, timeoutSecs=120, retryDelaySecs=1.0, initialDelaySecs=None, pollTimeoutSecs=180,
    noise=None, benchmarkLogging=None, noPoll=False,
    print_params=True, noPrint=False, **kwargs):

    params_dict = {
        'source': None,
        'destination_key': None,
        'k_max': None,
        'b_max': None,
        'bootstrap_fraction': None,
        'seed': None,
        'cols': None,
        'ignored_cols': None,
        'ignored_cols_by_name': None,
    }
    browseAlso = kwargs.pop('browseAlso', False)
    check_params_update_kwargs(params_dict, kwargs, 'gap_statistic', print_params=True)
    start = time.time()
    a = self.do_json_request('2/GapStatistic.json', timeout=timeoutSecs, params=params_dict)
    if noPoll:
        return a
    a = self.poll_url(a, timeoutSecs=timeoutSecs, retryDelaySecs=retryDelaySecs, benchmarkLogging=benchmarkLogging,
                      initialDelaySecs=initialDelaySecs, pollTimeoutSecs=pollTimeoutSecs)
    verboseprint("\ngap_statistic result:", dump_json(a))
    a['python_elapsed'] = time.time() - start
    a['python_%timeout'] = a['python_elapsed'] * 100 / timeoutSecs
    return a

def speedrf(self, data_key, ntrees=50, max_depth=20, timeoutSecs=300, 
    retryDelaySecs=1.0, initialDelaySecs=None, pollTimeoutSecs=180,
    noise=None, benchmarkLogging=None, noPoll=False,
    print_params=True, noPrint=False, **kwargs):

    params_dict = {
        'balance_classes': None,
        'classification': 1,
        'cols': None,
        'destination_key': None,
        'ignored_cols': None,
        'ignored_cols_by_name': None,
        'importance': 0,
        'keep_cross_validation_splits': None,
        'max_after_balance_size': None,
        'max_depth': max_depth,
        'mtries': -1.0,
        'nbins': 1024.0,
        'n_folds': None,
        'ntrees': ntrees,
        'oobee': 0,
        'response': None,
        'sample_rate': 0.67,
        'sampling_strategy': 'RANDOM',
        'score_pojo': None, # create the score pojo
        'seed': -1.0,
        'select_stat_type': 'ENTROPY', # GINI
        'source': data_key,
        'validation': None,
        'verbose': None,
    }
    check_params_update_kwargs(params_dict, kwargs, 'SpeeDRF', print_params)

    if print_params:
        print "\n%s parameters:" % "SpeeDRF", params_dict
        sys.stdout.flush()

    rf = self.do_json_request('2/SpeeDRF.json', timeout=timeoutSecs, params=params_dict)
    print "\n%s result:" % "SpeeDRF", dump_json(rf)

    if noPoll:
        print "Not polling SpeeDRF"
        return rf

    time.sleep(2)
    rfView = self.poll_url(rf, timeoutSecs=timeoutSecs, retryDelaySecs=retryDelaySecs,
        initialDelaySecs=initialDelaySecs, pollTimeoutSecs=pollTimeoutSecs,
        noise=noise, benchmarkLogging=benchmarkLogging, noPrint=noPrint)
    return rfView

# note ntree in kwargs can overwrite trees! (trees is legacy param)
def random_forest(self, data_key, trees=None,
    timeoutSecs=300, retryDelaySecs=1.0, initialDelaySecs=None, pollTimeoutSecs=180,
    noise=None, benchmarkLogging=None, noPoll=False, rfView=True,
    print_params=True, noPrint=False, **kwargs):

    print "at top of random_forest, timeoutSec: ", timeoutSecs
    algo = '2/DRF'
    algoView = '2/DRFView'

    params_dict = {
        # 'model': None,
        'balance_classes': None, 
        'build_tree_one_node': None,
        'classification': 1,
        'cols': None,
        'destination_key': None,
        'ignored_cols': None,
        'ignored_cols_by_name': None,
        'importance': 1, # enable variable importance by default
        'max_after_balance_size': None,
        'max_depth': None,
        'min_rows': None, # how many rows in leaves for stopping condition
        'mtries': None,
        'nbins': None,
        'ntrees': trees,
        'n_folds': None,
        'response': None,
        'sample_rate': None,
        'score_each_iteration': None,
        'seed': None,
        'source': data_key,
        'validation': None,
    }
    if 'model_key' in kwargs:
        kwargs['destination_key'] = kwargs['model_key'] # hmm..should we switch test to new param?

    browseAlso = kwargs.pop('browseAlso', False)
    check_params_update_kwargs(params_dict, kwargs, 'random_forest', print_params)

    # on v2, there is no default response. So if it's none, we should use the last column, for compatibility
    inspect = h2o_cmd.runInspect(key=data_key)
    # response only takes names. can't use col index..have to look it up
    # or add last col
    # mnist can be col 0 for response!
    if ('response' not in params_dict) or (params_dict['response'] is None):
        params_dict['response'] = str(inspect['cols'][-1]['name'])
    elif isinstance(params_dict['response'], int): 
        params_dict['response'] = str(inspect['cols'][params_dict['response']]['name'])

    if print_params:
        print "\n%s parameters:" % algo, params_dict
        sys.stdout.flush()

    # always follow thru to rfview?
    rf = self.do_json_request(algo + '.json', timeout=timeoutSecs, params=params_dict)
    print "\n%s result:" % algo, dump_json(rf)

    # noPoll and rfView=False are similar?
    if (noPoll or not rfView):
        # just return for now
        print "no rfView:", rfView, "noPoll", noPoll
        return rf

    # since we don't know the model key from the rf response, we just let rf redirect us to completion
    # if we want to do noPoll, we have to name the model, so we know what to ask for when we do the completion view
    # HACK: wait more for first poll?
    time.sleep(5)
    rfView = self.poll_url(rf, timeoutSecs=timeoutSecs, retryDelaySecs=retryDelaySecs,
        initialDelaySecs=initialDelaySecs, pollTimeoutSecs=pollTimeoutSecs,
        noise=noise, benchmarkLogging=benchmarkLogging, noPrint=noPrint)
    return rfView

def random_forest_view(self, data_key=None, model_key=None, timeoutSecs=300,
    retryDelaySecs=0.2, initialDelaySecs=None, pollTimeoutSecs=180,
    noise=None, benchmarkLogging=None, print_params=False, noPoll=False,
    noPrint=False, **kwargs):

    print "random_forest_view not supported in H2O fvec yet. hacking done response"
    r = {'response': {'status': 'done'}, 'trees': {'number_built': 0}}
        # return r

    algo = '2/DRFModelView'
    # No such thing as 2/DRFScore2
    algoScore = '2/DRFScore2'
    # is response_variable needed here? it shouldn't be
    # do_json_request will ignore any that remain = None

    params_dict = {
        '_modelKey': model_key,
    }
    browseAlso = kwargs.pop('browseAlso', False)

    # only update params_dict..don't add
    # throw away anything else as it should come from the model (propagating what RF used)
    for k in kwargs:
        if k in params_dict:
            params_dict[k] = kwargs[k]

    if print_params:
        print "\n%s parameters:" % algo, params_dict
        sys.stdout.flush()

    whichUsed = algo
    # for drf2, you can't pass a new dataset here, compared to what you trained with.
    # should complain or something if tried with a data_key
    if data_key:
        print "Can't pass a new data_key to random_forest_view for v2's DRFModelView. Not using"

    a = self.do_json_request(whichUsed + ".json", timeout=timeoutSecs, params=params_dict)
    verboseprint("\n%s result:" % whichUsed, dump_json(a))

    if noPoll:
        return a

    # add a fake redirect_request and redirect_request_args
    # to the RF response, to make it look like everyone else
    rfView = self.poll_url(a, timeoutSecs=timeoutSecs, retryDelaySecs=retryDelaySecs,
        initialDelaySecs=initialDelaySecs, pollTimeoutSecs=pollTimeoutSecs,
        noPrint=noPrint, noise=noise, benchmarkLogging=benchmarkLogging)

    drf_model = rfView['drf_model']
    numberBuilt = drf_model['N']

    # want to double check all this because it's new
    # and we had problems with races/doneness before
    errorInResponse = False
    # numberBuilt<0 or ntree<0 or numberBuilt>ntree or \
    # ntree!=rfView['ntree']

    if errorInResponse:
        raise Exception("\nBad values in %s.json\n" % whichUsed +
            "progress: %s, progressTotal: %s, ntree: %s, numberBuilt: %s, status: %s" % \
            (progress, progressTotal, ntree, numberBuilt, status))

    if (browseAlso | h2o_args.browse_json):
        h2b.browseJsonHistoryAsUrlLastMatch(whichUsed)
    return rfView

def set_column_names(self, timeoutSecs=300, print_params=False, **kwargs):
    params_dict = {
        'copy_from': None,
        'source': None,
        'cols': None,
        'comma_separated_list': None,
    }
    check_params_update_kwargs(params_dict, kwargs, 'set_column_names', print_params)
    a = self.do_json_request('2/SetColumnNames2.json', timeout=timeoutSecs, params=params_dict)
    verboseprint("\nset_column_names result:", dump_json(a))
    return a

def quantiles(self, timeoutSecs=300, print_params=True, **kwargs):
    params_dict = {
        'source_key': None,
        'column': None,
        'quantile': None,
        'max_qbins': None,
        'interpolation_type': None,
        'multiple_pass': None,
    }
    check_params_update_kwargs(params_dict, kwargs, 'quantiles', print_params)
    a = self.do_json_request('2/QuantilesPage.json', timeout=timeoutSecs, params=params_dict)
    verboseprint("\nquantiles result:", dump_json(a))
    return a

def anomaly(self, timeoutSecs=300, retryDelaySecs=1, initialDelaySecs=5, pollTimeoutSecs=30,
    noPoll=False, print_params=True, benchmarkLogging=None, **kwargs):
    params_dict = {
        'destination_key': None,
        'source': None,
        'dl_autoencoder_model': None,
        'thresh': -1,
    }
    check_params_update_kwargs(params_dict, kwargs, 'anomaly', print_params)
    a = self.do_json_request('2/Anomaly.json', timeout=timeoutSecs, params=params_dict)

    if noPoll:
        return a

    a = self.poll_url(a, timeoutSecs=timeoutSecs, retryDelaySecs=retryDelaySecs, benchmarkLogging=benchmarkLogging,
        initialDelaySecs=initialDelaySecs, pollTimeoutSecs=pollTimeoutSecs)

    verboseprint("\nanomaly result:", dump_json(a))
    return a

def deep_features(self, timeoutSecs=300, retryDelaySecs=1, initialDelaySecs=5, pollTimeoutSecs=30,
    noPoll=False, print_params=True, benchmarkLogging=None, **kwargs):
    params_dict = {
        'destination_key': None,
        'source': None,
        'dl_model': None,
        'layer': -1,
    }
    check_params_update_kwargs(params_dict, kwargs, 'deep_features', print_params)
    a = self.do_json_request('2/DeepFeatures.json', timeout=timeoutSecs, params=params_dict)

    if noPoll:
        return a

    a = self.poll_url(a, timeoutSecs=timeoutSecs, retryDelaySecs=retryDelaySecs, benchmarkLogging=benchmarkLogging,
        initialDelaySecs=initialDelaySecs, pollTimeoutSecs=pollTimeoutSecs)

    verboseprint("\ndeep_features result:", dump_json(a))
    return a


def naive_bayes(self, timeoutSecs=300, retryDelaySecs=1, initialDelaySecs=5, pollTimeoutSecs=30,
    noPoll=False, print_params=True, benchmarkLogging=None, **kwargs):
    params_dict = {
        'destination_key': None,
        'source': None,
        'response': None,
        'cols': None,
        'ignored_cols': None,
        'ignored_cols_by_name': None,
        'laplace': None,
        'drop_na_cols': None,
        'min_std_dev': None,
    }
    check_params_update_kwargs(params_dict, kwargs, 'naive_bayes', print_params)
    a = self.do_json_request('2/NaiveBayes.json', timeout=timeoutSecs, params=params_dict)

    if noPoll:
        return a

    a = self.poll_url(a, timeoutSecs=timeoutSecs, retryDelaySecs=retryDelaySecs, benchmarkLogging=benchmarkLogging,
        initialDelaySecs=initialDelaySecs, pollTimeoutSecs=pollTimeoutSecs)

    verboseprint("\nnaive_bayes result:", dump_json(a))
    return a

def anomaly(self, timeoutSecs=300, retryDelaySecs=1, initialDelaySecs=5, pollTimeoutSecs=30,
    noPoll=False, print_params=True, benchmarkLogging=None, **kwargs):
    params_dict = {
        'destination_key': None,
        'source': None,
        'dl_autoencoder_model': None,
        'thresh': None,
    }
    check_params_update_kwargs(params_dict, kwargs, 'anomaly', print_params)
    start = time.time()
    a = self.do_json_request('2/Anomaly.json', timeout=timeoutSecs, params=params_dict)

    if noPoll:
        return a

    a = self.poll_url(a, timeoutSecs=timeoutSecs, retryDelaySecs=retryDelaySecs, benchmarkLogging=benchmarkLogging,
        initialDelaySecs=initialDelaySecs, pollTimeoutSecs=pollTimeoutSecs)
    verboseprint("\nanomaly :result:", dump_json(a))
    a['python_elapsed'] = time.time() - start
    a['python_%timeout'] = a['python_elapsed'] * 100 / timeoutSecs
    return a

def gbm_view(self, model_key, timeoutSecs=300, print_params=False, **kwargs):
    params_dict = {
        '_modelKey': model_key,
    }
    # only lets these params thru
    check_params_update_kwargs(params_dict, kwargs, 'gbm_view', print_params)
    a = self.do_json_request('2/GBMModelView.json', timeout=timeoutSecs, params=params_dict)
    verboseprint("\ngbm_view result:", dump_json(a))
    return a

def gbm_grid_view(self, timeoutSecs=300, print_params=False, **kwargs):
    params_dict = {
        'job_key': None,
        'destination_key': None,
    }
    # only lets these params thru
    check_params_update_kwargs(params_dict, kwargs, 'gbm_grid_view', print_params)
    a = self.do_json_request('2/GridSearchProgress.json', timeout=timeoutSecs, params=params_dict)
    print "\ngbm_grid_view result:", dump_json(a)
    return a

def speedrf_view(self, modelKey, timeoutSecs=300, print_params=False, **kwargs):
    params_dict = { '_modelKey': modelKey, }
    check_params_update_kwargs(params_dict, kwargs, 'speedrf_view', print_params)
    a = self.do_json_request('2/SpeeDRFModelView.json', timeout=timeoutSecs, params=params_dict)
    verboseprint("\nspeedrf_view_result:", dump_json(a))
    return a

def speedrf_grid_view(self, timeoutSecs=300, print_params=False, **kwargs):
    params_dict = {
        'job_key': None,
        'destination_key': None,
    }
    # only lets these params thru
    check_params_update_kwargs(params_dict, kwargs, 'speedrf_grid_view', print_params)
    a = self.do_json_request('2/GridSearchProgress.json', timeout=timeoutSecs, params=params_dict)
    print "\nspeedrf_grid_view result:", dump_json(a)
    return a

def pca_view(self, modelKey, timeoutSecs=300, print_params=False, **kwargs):
    #this function is only for pca on fvec! may replace in future.
    params_dict = {
        '_modelKey': modelKey,
    }
    check_params_update_kwargs(params_dict, kwargs, 'pca_view', print_params)
    a = self.do_json_request('2/PCAModelView.json', timeout=timeoutSecs, params=params_dict)
    verboseprint("\npca_view_result:", dump_json(a))
    return a

def glm_grid_view(self, timeoutSecs=300, print_params=False, **kwargs):
    #this function is only for glm2, may remove it in future.
    params_dict = {
        'grid_key': None,
    }
    check_params_update_kwargs(params_dict, kwargs, 'glm_grid_view', print_params)
    a = self.do_json_request('2/GLMGridView.json', timeout=timeoutSecs, params=params_dict)
    verboseprint("\nglm_grid_view result:", dump_json(a))
    return a

def glm_view(self, modelKey=None, timeoutSecs=300, print_params=False, **kwargs):
    #this function is only for glm2, may remove it in future.
    params_dict = {
        '_modelKey': modelKey,
    }
    check_params_update_kwargs(params_dict, kwargs, 'glm_view', print_params)
    a = self.do_json_request('2/GLMModelView.json', timeout=timeoutSecs, params=params_dict)
    verboseprint("\nglm_view result:", dump_json(a))
    return a

def save_model(self, timeoutSecs=300, print_params=False, **kwargs):
    #this function is only for glm2, may remove it in future.
    params_dict = {
        'model': None,
        'path': None,
        'force': None,
    }
    check_params_update_kwargs(params_dict, kwargs, 'save_model', print_params)
    a = self.do_json_request('2/SaveModel.json', timeout=timeoutSecs, params=params_dict)
    verboseprint("\nsave_model result:", dump_json(a))
    return a

def load_model(self, timeoutSecs=300, print_params=False, **kwargs):
    params_dict = {
        'path': None,
    }
    check_params_update_kwargs(params_dict, kwargs, 'load_model', print_params)
    a = self.do_json_request('2/LoadModel.json', timeout=timeoutSecs, params=params_dict)
    verboseprint("\nload_model result:", dump_json(a))
    return a

def generate_predictions(self, data_key, model_key, destination_key=None, timeoutSecs=300, print_params=True,
                         **kwargs):
    algo = '2/Predict'
    algoView = '2/Inspect2'

    params_dict = {
        'data': data_key,
        'model': model_key,
        # 'prediction_key': destination_key,
        'prediction': destination_key,
    }
    browseAlso = kwargs.pop('browseAlso', False)
    # only lets these params thru
    check_params_update_kwargs(params_dict, kwargs, 'generate_predictions', print_params)

    if print_params:
        print "\n%s parameters:" % algo, params_dict
        sys.stdout.flush()

    a = self.do_json_request(
        algo + '.json',
        timeout=timeoutSecs,
        params=params_dict)
    verboseprint("\n%s result:" % algo, dump_json(a))

    if (browseAlso | h2o_args.browse_json):
        h2b.browseJsonHistoryAsUrlLastMatch(algo)

    return a

def predict_confusion_matrix(self, timeoutSecs=300, print_params=True, **kwargs):
    params_dict = {
        'actual': None,
        'vactual': 'predict',
        'predict': None,
        'vpredict': 'predict',
    }
    # everyone should move to using this, and a full list in params_dict
    # only lets these params thru
    check_params_update_kwargs(params_dict, kwargs, 'predict_confusion_matrix', print_params)
    a = self.do_json_request('2/ConfusionMatrix.json', timeout=timeoutSecs, params=params_dict)
    verboseprint("\nprediction_confusion_matrix result:", dump_json(a))
    return a

def hit_ratio(self, timeoutSecs=300, print_params=True, **kwargs):
    params_dict = {
        'actual': None,
        'vactual': 'predict',
        'predict': None,
        'max_k': seed,
        'make_k': 'None',
    }
    check_params_update_kwargs(params_dict, kwargs, 'auc', print_params)
    a = self.do_json_request('2/HitRatio.json', timeout=timeoutSecs, params=params_dict)
    verboseprint("\nhit_ratio result:", dump_json(a))
    return a

def generate_auc(self, timeoutSecs=300, print_params=True, **kwargs):
    params_dict = {
        'thresholds': None,
        'actual': None,
        'vactual': 'predict',
        'predict': None,
        'vpredict': 'predict',
    }
    check_params_update_kwargs(params_dict, kwargs, 'auc', print_params)
    a = self.do_json_request('2/AUC.json', timeout=timeoutSecs, params=params_dict)
    verboseprint("\nauc result:", dump_json(a))
    return a

def gbm(self, data_key, timeoutSecs=600, retryDelaySecs=1, initialDelaySecs=5, pollTimeoutSecs=30,
        noPoll=False, print_params=True, **kwargs):
    params_dict = {
        'balance_classes': None,
        'checkpoint': None,
        'classification': None,
        'class_sampling_factors': None,
        'cols': None,
        'destination_key': None,
        'distribution': None, # multinomial is a choice
        'family': None, # can be 'bernoulli' or 'AUTO'
        'grid_parallelism': None,
        'group_split': None,
        'grid_parallelism': None,
        'group_split': None, # categoricals
        'holdout_fraction': None,
        'ignored_cols': None,
        'ignored_cols_by_name': None, # either this or cols..not both
        'importance': None,
        'keep_cross_validation_splits': None,
        'learn_rate': None,
        'max_depth': None,
        'max_after_balance_size': None,
        'min_rows': None,
        'nbins': None,
        'ntrees': None,
        'overwrite_checkpoint': None,
        'response': None,
        'score_each_iteration': None,
        'seed': None,
        'source': data_key,
        'validation': None,
    }

    # only lets these params thru
    check_params_update_kwargs(params_dict, kwargs, 'gbm', print_params)
    if 'validation' not in kwargs:
        kwargs['validation'] = data_key

    start = time.time()
    a = self.do_json_request('2/GBM.json', timeout=timeoutSecs, params=params_dict)
    if noPoll:
        a['python_elapsed'] = time.time() - start
        a['python_%timeout'] = a['python_elapsed'] * 100 / timeoutSecs
        return a

    verboseprint("\nGBM first result:", dump_json(a))
    a = self.poll_url(a, timeoutSecs=timeoutSecs, retryDelaySecs=retryDelaySecs,
                      initialDelaySecs=initialDelaySecs, pollTimeoutSecs=pollTimeoutSecs)
    verboseprint("\nGBM result:", dump_json(a))
    a['python_elapsed'] = time.time() - start
    a['python_%timeout'] = a['python_elapsed'] * 100 / timeoutSecs
    return a

def pca(self, data_key, timeoutSecs=600, retryDelaySecs=1, initialDelaySecs=5, pollTimeoutSecs=30,
        noPoll=False, print_params=True, benchmarkLogging=None, returnFast=False, **kwargs):
    params_dict = {
        'destination_key': None,
        'source': data_key,
        'cols': None,
        'ignored_cols': None,
        'ignored_col_names': None,
        'tolerance': None,
        'max_pc': None,
        'standardize': None,
    }
    # only lets these params thru
    check_params_update_kwargs(params_dict, kwargs, 'pca', print_params)
    start = time.time()
    a = self.do_json_request('2/PCA.json', timeout=timeoutSecs, params=params_dict, returnFast=returnFast)

    if noPoll:
        #a['python_elapsed'] = time.time() - start
        #a['python_%timeout'] = a['python_elapsed']*100 / timeoutSecs
        return a

    a = self.poll_url(a, timeoutSecs=timeoutSecs, retryDelaySecs=retryDelaySecs, benchmarkLogging=benchmarkLogging,
                      initialDelaySecs=initialDelaySecs, pollTimeoutSecs=pollTimeoutSecs)
    verboseprint("\nPCA result:", dump_json(a))
    a['python_elapsed'] = time.time() - start
    a['python_%timeout'] = a['python_elapsed'] * 100 / timeoutSecs
    return a

def pca_score(self, timeoutSecs=600, retryDelaySecs=1, initialDelaySecs=5, pollTimeoutSecs=30,
              noPoll=False, print_params=True, **kwargs):
    params_dict = {
        'model': None,
        'destination_key': None,
        'source': None,
        'num_pc': None,
    }
    # only lets these params thru
    check_params_update_kwargs(params_dict, kwargs, 'pca_score', print_params)
    start = time.time()
    a = self.do_json_request('2/PCAScore.json', timeout=timeoutSecs, params=params_dict)

    if noPoll:
        a['python_elapsed'] = time.time() - start
        a['python_%timeout'] = a['python_elapsed'] * 100 / timeoutSecs
        return a

    if 'response' not in a:
        raise Exception("Can't tell where to go..No 'response' key in this polled json response: %s" % a)

    a = self.poll_url(a, timeoutSecs=timeoutSecs, retryDelaySecs=retryDelaySecs,
                      initialDelaySecs=initialDelaySecs, pollTimeoutSecs=pollTimeoutSecs)
    verboseprint("\nPCAScore result:", dump_json(a))
    a['python_elapsed'] = time.time() - start
    a['python_%timeout'] = a['python_elapsed'] * 100 / timeoutSecs
    return a

def neural_net_score(self, key, model, timeoutSecs=60, retryDelaySecs=1, initialDelaySecs=5, pollTimeoutSecs=30,
                     noPoll=False, print_params=True, **kwargs):
    params_dict = {
        'source': key,
        'destination_key': None,
        'model': model,
        'cols': None,
        'ignored_cols': None,
        'ignored_col_name': None,
        'classification': None,
        'response': None,
        'max_rows': 0,
    }
    # only lets these params thru
    check_params_update_kwargs(params_dict, kwargs, 'neural_net_score', print_params)

    start = time.time()
    a = self.do_json_request('2/NeuralNetScore.json', timeout=timeoutSecs, params=params_dict)

    if noPoll:
        a['python_elapsed'] = time.time() - start
        a['python_%timeout'] = a['python_elapsed'] * 100 / timeoutSecs
        return a

    # no polling
    # a = self.poll_url(a, timeoutSecs=timeoutSecs, retryDelaySecs=retryDelaySecs,
    #                   initialDelaySecs=initialDelaySecs, pollTimeoutSecs=pollTimeoutSecs)
    verboseprint("\nneural net score result:", dump_json(a))
    a['python_elapsed'] = time.time() - start
    a['python_%timeout'] = a['python_elapsed'] * 100 / timeoutSecs
    return a

def neural_net(self, data_key, timeoutSecs=60, retryDelaySecs=1, initialDelaySecs=5, pollTimeoutSecs=30,
               noPoll=False, print_params=True, **kwargs):
    params_dict = {
        'destination_key': None,
        'source': data_key,
        'cols': None,
        'ignored_cols': None,
        'ignored_cols_by_name': None,
        'validation': None,
        'classification': None,
        'response': None,
        'mode': None,
        'activation': None,
        'input_dropout_ratio': None,
        'hidden': None,
        'rate': None,
        'rate_annealing': None,
        'momentum_start': None,
        'momentum_ramp': None,
        'momentum_stable': None,
        'l1': None,
        'l2': None,
        'seed': None,
        'loss': None,
        'max_w2': None,
        'warmup_samples': None,
        'initial_weight_distribution': None,
        'initial_weight_scale': None,
        'epochs': None,
    }
    # only lets these params thru
    check_params_update_kwargs(params_dict, kwargs, 'neural_net', print_params)
    if 'validation' not in kwargs:
        kwargs['validation'] = data_key

    start = time.time()
    a = self.do_json_request('2/NeuralNet.json', timeout=timeoutSecs, params=params_dict)

    if noPoll:
        a['python_elapsed'] = time.time() - start
        a['python_%timeout'] = a['python_elapsed'] * 100 / timeoutSecs
        return a

    a = self.poll_url(a, timeoutSecs=timeoutSecs, retryDelaySecs=retryDelaySecs,
                      initialDelaySecs=initialDelaySecs, pollTimeoutSecs=pollTimeoutSecs)
    verboseprint("\nneural_net result:", dump_json(a))
    a['python_elapsed'] = time.time() - start
    a['python_%timeout'] = a['python_elapsed'] * 100 / timeoutSecs
    return a

def deep_learning(self, data_key, timeoutSecs=60, retryDelaySecs=1, initialDelaySecs=5, pollTimeoutSecs=30,
                  noPoll=False, print_params=True, **kwargs):
    params_dict = {
        'autoencoder': None,
        'destination_key': None,
        'source': data_key,
        'cols': None,
        'ignored_cols': None,
        'ignored_cols_by_name': None,
        'validation': None,
        'classification': None,
        'response': None,
        'expert_mode': None,
        'activation': None,
        'hidden': None,
        'epochs': None,
        'train_samples_per_iteration': None,
        'seed': None,
        'adaptive_rate': None,
        'rho': None,
        'epsilon': None,
        'rate': None,
        'rate_annealing': None,
        'rate_decay': None,
        'momentum_start': None,
        'momentum_ramp': None,
        'momentum_stable': None,
        'nesterov_accelerated_gradient': None,
        'input_dropout_ratio': None,
        'hidden_dropout_ratios': None,
        'l1': None,
        'l2': None,
        'max_w2': None,
        'initial_weight_distribution': None,
        'initial_weight_scale': None,
        'loss': None,
        'score_interval': None,
        'score_training_samples': None,
        'score_validation_samples': None,
        'score_duty_cycle': None,
        'classification_stop': None,
        'regression_stop': None,
        'quiet_mode': None,
        'max_confusion_matrix_size': None,
        'max_hit_ratio_k': None,
        'balance_classes': None,
        'max_after_balance_size': None,
        'score_validation_sampling': None,
        'diagnostics': None,
        'variable_importances': None,
        'fast_mode': None,
        'ignore_const_cols': None,
        'force_load_balance': None,
        'replicate_training_data': None,
        'single_node_mode': None,
        'shuffle_training_data': None,
        'n_folds': None,
    }
    # only lets these params thru
    check_params_update_kwargs(params_dict, kwargs, 'deep_learning', print_params)
    if 'validation' not in kwargs:
        kwargs['validation'] = data_key

    start = time.time()
    a = self.do_json_request('2/DeepLearning.json', timeout=timeoutSecs, params=params_dict)

    if noPoll:
        a['python_elapsed'] = time.time() - start
        a['python_%timeout'] = a['python_elapsed'] * 100 / timeoutSecs
        return a

    a = self.poll_url(a, timeoutSecs=timeoutSecs, retryDelaySecs=retryDelaySecs,
        initialDelaySecs=initialDelaySecs, pollTimeoutSecs=pollTimeoutSecs)
    verboseprint("\nneural_net result:", dump_json(a))
    a['python_elapsed'] = time.time() - start
    a['python_%timeout'] = a['python_elapsed'] * 100 / timeoutSecs
    return a

def neural_view(self, model_key, timeoutSecs=300, print_params=False, **kwargs):
    params_dict = {
        'destination_key': model_key,
    }
    # only lets these params thru
    check_params_update_kwargs(params_dict, kwargs, 'nn_view', print_params)
    a = self.do_json_request('2/NeuralNetProgress.json', timeout=timeoutSecs, params=params_dict)
    verboseprint("\nneural_view result:", dump_json(a))
    return a

def summary_page(self, key, timeoutSecs=60, noPrint=True, useVA=False, numRows=None, numCols=None, **kwargs):
    params_dict = {
        'source': key,
        'cols': None, # is this zero based like everything else?
        'max_ncols': 1000 if not numCols else numCols,
        'max_qbins': None,
    }
    browseAlso = kwargs.pop('browseAlso', False)
    check_params_update_kwargs(params_dict, kwargs, 'summary_page', print_params=True)
    a = self.do_json_request('2/SummaryPage2.json', timeout=timeoutSecs, params=params_dict)
    h2o_cmd.infoFromSummary(a, noPrint=noPrint, numRows=numRows, numCols=numCols)
    return a

def log_view(self, timeoutSecs=10, **kwargs):
    browseAlso = kwargs.pop('browseAlso', False)
    a = self.do_json_request('LogView.json', timeout=timeoutSecs)
    verboseprint("\nlog_view result:", dump_json(a))
    if (browseAlso | h2o_args.browse_json):
        h2b.browseJsonHistoryAsUrlLastMatch("LogView")
        time.sleep(3) # to be able to see it
    return a

def csv_download(self, src_key, csvPathname, timeoutSecs=60, **kwargs):
    # log it
    params = {'src_key': src_key}
    paramsStr = '?' + '&'.join(['%s=%s' % (k, v) for (k, v) in params.items()])
    url = self.url('2/DownloadDataset.json')
    log('Start ' + url + paramsStr, comment=csvPathname)

    # do it (absorb in 1024 byte chunks)
    r = requests.get(url, params=params, timeout=timeoutSecs)
    print "csv_download r.headers:", r.headers
    if r.status_code == 200:
        f = open(csvPathname, 'wb')
        for chunk in r.iter_content(1024):
            f.write(chunk)
    print csvPathname, "size:", h2o_util.file_size_formatted(csvPathname)

# shouldn't need params
def log_download(self, logDir=None, timeoutSecs=30, **kwargs):
    if logDir == None:
        logDir = get_sandbox_name()

    url = self.url('LogDownload.json')
    log('Start ' + url);
    print "\nDownloading h2o log(s) using:", url
    r = requests.get(url, timeout=timeoutSecs, **kwargs)
    if not r or not r.ok:
        raise Exception("Maybe bad url? no r in log_download %s in %s:" % inspect.stack()[1][3])

    z = zipfile.ZipFile(StringIO.StringIO(r.content))
    print "z.namelist:", z.namelist()
    print "z.printdir:", z.printdir()

    nameList = z.namelist()
    # the first is the h2ologs dir name.
    h2oLogDir = logDir + "/" + nameList.pop(0)
    print "h2oLogDir:", h2oLogDir
    print "logDir:", logDir

    # it's a zip of zipped files
    # first unzip it
    z = zipfile.ZipFile(StringIO.StringIO(r.content))
    z.extractall(logDir)
    # unzipped file should be in LOG_DIR now
    # now unzip the files in that directory
    for zname in nameList:
        resultList = h2o_util.flat_unzip(logDir + "/" + zname, logDir)

    print "\nlogDir:", logDir
    for logfile in resultList:
        numLines = sum(1 for line in open(logfile))
        print logfile, "Lines:", numLines
    print
    return resultList


# kwargs used to pass many params
def GLM_shared(self, key,
    timeoutSecs=300, retryDelaySecs=0.5, initialDelaySecs=None, pollTimeoutSecs=180,
    parentName=None, **kwargs):

    browseAlso = kwargs.pop('browseAlso', False)
    params_dict = {
        'alpha': None,
        'beta_epsilon': None, # GLMGrid doesn't use this name
        'beta_constraints': None, 
        'cols': None,
        'destination_key': None,
        'disable_line_search': None,
        'family': None,
        'intercept': None, # use intercept in the model
        'higher_accuracy': None, # use line search (use if no convergence otherwise)
        'ignored_cols': None,
        'ignored_cols_by_name': None,
        'lambda': None,
        'lambda_min_ratio': None, # min lambda used in lambda search, ratio of lambda_max
        'lambda_search': None, # use lambda search, start at lambda max. lambda is used as lambda min
        'link': None,
        'max_iter': None,
        'max_predictors': None, # lambda_search stop condition. Stop when more than this # of predictors.
        'n_folds': None,
        'nlambdas': None, # number of lambdas to be used in a search
        'non_negative': None, # require coefficients to be non-negative
        'prior': None, # prior probability for y=1. For logistic, if the data is sampled and mean is skewed
        'response': None,
        'source': key,
        'standardize': None,
        'strong_rules': None, # use strong rules to filter out inactive columns
        'tweedie_variance_power': None,
        'use_all_factor_levels': None, # normally first factor is skipped. Set to use all levels.
        'variable_importances': None, # if use_all_factor_levels is off, base level is not shown

    }

    check_params_update_kwargs(params_dict, kwargs, parentName, print_params=True)
    a = self.do_json_request(parentName + '.json', timeout=timeoutSecs, params=params_dict)
    verboseprint(parentName, dump_json(a))
    return a

def GLM(self, key,
        timeoutSecs=300, retryDelaySecs=0.5, initialDelaySecs=None, pollTimeoutSecs=180,
        noise=None, benchmarkLogging=None, noPoll=False, destination_key=None, **kwargs):
    parentName = "2/GLM2"
    a = self.GLM_shared(key, timeoutSecs, retryDelaySecs, initialDelaySecs, parentName=parentName,
                        destination_key=destination_key, **kwargs)
    # Check that the response has the right Progress url it's going to steer us to.
    if noPoll:
        return a

    a = self.poll_url(a, timeoutSecs=timeoutSecs, retryDelaySecs=retryDelaySecs,
        initialDelaySecs=initialDelaySecs, pollTimeoutSecs=pollTimeoutSecs,
        noise=noise, benchmarkLogging=benchmarkLogging)
    verboseprint("GLM done:", dump_json(a))

    browseAlso = kwargs.get('browseAlso', False)
    if (browseAlso | h2o_args.browse_json):
        print "Viewing the GLM result through the browser"
        h2b.browseJsonHistoryAsUrlLastMatch('GLMProgressPage')
        time.sleep(5)
    return a

def GLMGrid_view(self, timeoutSecs=300, print_params=False, **kwargs):
    params_dict = {
        'job': None,
        'destination_key': None,
    }
    # only lets these params thru
    check_params_update_kwargs(params_dict, kwargs, 'GLMGridProgress', print_params)
    a = self.do_json_request('GLMGridProgress.json', timeout=timeoutSecs, params=params_dict)
    print "\nGLMGridProgress result:", dump_json(a)
    return a

# GLMScore params
# model_key=__GLMModel_7a3a73c1-f272-4a2e-b37f-d2f371d304ba&
# key=cuse.hex&
# thresholds=0%3A1%3A0.01
def GLMScore(self, key, model_key, timeoutSecs=100, **kwargs):
    # this isn't in fvec?
    browseAlso = kwargs.pop('browseAlso', False)
    # i guess key and model_key could be in kwargs, but
    # maybe separate is more consistent with the core key behavior
    # elsewhere
    params_dict = {
        'key': key,
        'model_key': model_key,
    }
    params_dict.update(kwargs)
    print "\nGLMScore params list:", params_dict

    a = self.do_json_request('GLMScore.json', timeout=timeoutSecs, params=params_dict)
    verboseprint("GLMScore:", dump_json(a))

    browseAlso = kwargs.get('browseAlso', False)
    if (browseAlso | h2o_args.browse_json):
        print "Redoing the GLMScore through the browser, no results saved though"
        h2b.browseJsonHistoryAsUrlLastMatch('GLMScore')
        time.sleep(5)
    return a

def models(self, timeoutSecs=10, **kwargs):
    params_dict = {
        'key': None,
        'find_compatible_frames': 0,
        'score_frame': None
    }
    check_params_update_kwargs(params_dict, kwargs, 'models', True)
    result = self.do_json_request('2/Models', timeout=timeoutSecs, params=params_dict)
    return result

def frames(self, timeoutSecs=10, **kwargs):
    params_dict = {
        'key': None,
        'find_compatible_models': 0,
        'score_model': None
    }
    check_params_update_kwargs(params_dict, kwargs, 'frames', True)
    result = self.do_json_request('2/Frames', timeout=timeoutSecs, params=params_dict)
    return result

#FIX! just here temporarily to get the response at the end of an algo, from job/destination_key
def completion_redirect(self, jsonRequest, params):
    return self.do_json_request(jsonRequest=jsonRequest, params=params)


#******************************************************************************************8

# attach methods to H2O object
# this happens before any H2O instances are created
# this file is imported into h2o

H2O.anomaly = anomaly
H2O.completion_redirect = completion_redirect
H2O.create_frame = create_frame
H2O.csv_download = csv_download
H2O.deep_features = deep_features
H2O.deep_learning = deep_learning
H2O.exec_query = exec_query
H2O.export_files = export_files
H2O.export_hdfs = export_hdfs
H2O.export_s3 = export_s3
H2O.frames = frames
H2O.frame_split = frame_split
H2O.gap_statistic = gap_statistic
H2O.gbm = gbm
H2O.gbm_grid_view = gbm_grid_view
H2O.gbm_view = gbm_view
H2O.generate_auc = generate_auc
H2O.generate_predictions = generate_predictions
H2O.get_cloud = get_cloud
H2O.get_timeline = get_timeline
H2O.GLM = GLM
H2O.glm_grid_view = glm_grid_view
H2O.GLMGrid_view = GLMGrid_view
H2O.GLMScore = GLMScore
H2O.GLM_shared = GLM_shared
H2O.glm_view = glm_view
H2O.h2o_log_msg = h2o_log_msg
H2O.hit_ratio = hit_ratio
H2O.import_files = import_files
H2O.impute = impute
H2O.insert_missing_values = insert_missing_values
H2O.inspect = inspect
H2O.iostatus = iostatus
H2O.jobs_admin = jobs_admin
H2O.jobs_cancel = jobs_cancel
H2O.jprofile = jprofile
H2O.jstack = jstack
H2O.kmeans = kmeans
H2O.kmeans_view = kmeans_view
H2O.levels = levels
H2O.linux_info = linux_info
H2O.load_model = load_model
H2O.log_download = log_download
H2O.log_view = log_view
H2O.models = models
H2O.naive_bayes = naive_bayes
H2O.netstat = netstat
H2O.network_test = network_test
H2O.neural_net = neural_net
H2O.neural_net_score = neural_net_score
H2O.neural_view = neural_view
H2O.nfold_frame_extract = nfold_frame_extract
H2O.one_hot = one_hot
H2O.parse = parse
H2O.pca = pca
H2O.pca_score = pca_score
H2O.pca_view = pca_view
H2O.poll_url = poll_url
H2O.predict_confusion_matrix = predict_confusion_matrix
H2O.put_file = put_file
H2O.put_value = put_value
H2O.quantiles = quantiles
H2O.random_forest = random_forest
H2O.random_forest_view = random_forest_view
H2O.rebalance = rebalance
H2O.remove_all_keys = remove_all_keys
H2O.remove_key = remove_key
H2O.save_model = save_model
H2O.set_column_names = set_column_names
H2O.shutdown_all = shutdown_all
H2O.speedrf = speedrf
H2O.speedrf_grid_view = speedrf_grid_view
H2O.speedrf_view = speedrf_view
H2O.store_view = store_view
H2O.summary_page = summary_page
H2O.to_enum = to_enum
H2O.to_int = to_int
H2O.unlock = unlock
