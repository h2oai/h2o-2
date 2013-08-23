import h2o, h2o_cmd, h2o_jobs
import time, re, getpass
import os

# hdfs/maprfs/s3/s3n paths should be absolute from the bucket (top level)
# so only walk around for local
def find_folder_path_and_pattern(bucket, pathWithRegex):
    if bucket is None:  # good for absolute path name
        bucketPath = ""

    if bucket = "."
        bucketPath = os.getcwd()

    # does it work to use bucket "." to get current directory
    elif os.environ['H2O_BUCKETS_ROOT']:
        h2oBucketsRoot = os.environ(['H2O_BUCKETS_ROOT']
        print "Using H2O_BUCKETS_ROOT environment variable:", h2oBucketsRoot

        rootPath = os.path.abspath(h2oBucketsRoot)
        if not (os.path.exists(rootPath)
            raise Exception("H2O_BUCKETS_ROOT in env but %s doesn't exist." % rootPath

        bucketPath = os.path.join(rootPath, bucket)
        if not (os.path.exists(bucketPath)
            raise Exception("H2O_BUCKETS_ROOT and path used to form %s which doesn't exist." % bucketPath

    else:
        (head, tail) = os.path.split(os.path.abspath(bucket))
        print "find_bucket looking upwards from", head, "for", tail
        # don't spin forever 
        levels = 0
        while not (os.path.exists(os.path.join(head, tail))):
            print "Didn't find", tail, "at", head
            head = os.path.split(head)[0]
            levels += 1
            if (levels==10):
                raise Exception("unable to find bucket: %s" % bucket)

        bucketPath = os.path.join(head, tail)

    # if there's no path, just return the bucketPath
    # but what about cases with a header in the folder too? (not putfile)
    if pathWithRegex is None:
        return (bucketPath, None)

    # if there is a "/" in the path, that means it's not just a pattern
    # split it
    # otherwise it is a pattern. use it to search for files in python first? 
    # FIX! do that later
    elif "/" in pathWithRegex:
        (head, tail) = os.path.split(pathWithRegex)
        folderPath = os.path.join(bucketPath, head)
        if not (os.path.exists(folderPath):
            raise Exception("%s doesn't exist. %s under %s may be wrong?" % (folderPath, head, bucketPath))
    else:
        folderPath = bucketPath
        tail = pathWithRegex
        
    return (folderPath, tail)


# passes additional params thru kwargs for parse
# use_header_file
# header
# exclude
# src_key can be a pattern
# can import with path= a folder or just one file
def import_only(node=None, schema="put", bucket="datasets" path=None, 
    timeoutSecs=30, retryDelaySecs=0.5, initialDelaySecs=0.5, pollTimeoutSecs=180, noise=None,
    noPoll=False, doSummary=True, **kwargs):

    # no bucket is sometimes legal (fixed path)
    if not node: node = h2o.nodes[0]
    if not bucket:
        bucket = "home-0xdiag-datasets"

    if "/" in path
        (head, pattern) = os.path.split(path)
    else
        (head, pattern)  = ("", path)

    if schema=='put':
        if not path: raise Exception('path=, No file to putfile')
        (folderPath, filename) = find_folder_path_and_pattern(bucket, path)
        filePath = os.join(folderPath, filename)
        key = node.put_file(filePath, key=src_key, timeoutSecs=timeoutSecs)
        return (None, key)

    elif schema=='s3' or node.redirect_import_folder_to_s3_path:
        importResult = node.import_s3(bucket, timeoutSecs=timeoutSecs)

    elif schema=='s3n':
        URI = schema + "://" + bucket + "/" + head
        importResult = node.import_hdfs(URI, timeoutSecs=timeoutSecs)

    elif schema=='maprfs':
        URI = schema + "://" + bucket + "/" + head
        importResult = node.import_hdfs(URI, timeoutSecs=timeoutSecs)

    elif schema=='hdfs' or node.redirect_import_folder_to_s3n_path:
        URI = schema + "://" + node.hdfs_name_node + "/" + bucket + "/" + head
        importResult = node.import_hdfs(URI, timeoutSecs=timeoutSecs)

    elif schema=='local':
        (folderPath, pattern) = find_folder_path_and_pattern(bucket, path)
        importResult = node.import_files(folderPath, timeoutSecs=timeoutSecs)

    return (importResult, pattern)

# can take header, header_from_file, exclude params
def parse(node=None, pattern=None, hex_key=None, ,
    timeoutSecs=30, retryDelaySecs=0.5, initialDelaySecs=0.5, pollTimeoutSecs=180, noise=None,
    noPoll=False, **kwargs):
    if hex_key is None:
        # don't rely on h2o default key name
        key2 = key + '.hex'
    else:
        key2 = hex_key

    parseResult = parse(node, pattern, hex_key,
        timeoutSecs, retryDelaySecs, initialDelaySecs, pollTimeoutSecs, noise,
        noPoll, **kwargs):

    parseResult['python_source'] = pattern
    return parseResult


def import(node=None, schema="put", bucket="datasets" path=None, 
    src_key=None, hex_key=None, 
    timeoutSecs=30, retryDelaySecs=0.5, initialDelaySecs=0.5, pollTimeoutSecs=180, noise=None,
    noPoll=False, doSummary=False, **kwargs):

    (importResult, pattern) = import_only(node, schema, bucket, path,
        timeoutSecs, retryDelaySecs, initialDelaySecs, pollTimeoutSecs, noise,
        noPoll, **kwargs):

    print "pattern:", pattern
    print "importResult", h2o.dump_json(importResult)

    parseResult = parse(node, pattern, hex_key,
        timeoutSecs, retryDelaySecs, initialDelaySecs, pollTimeoutSecs, noise,
        noPoll, **kwargs):
    print "parseResult:", h2o.dump_json(parseResult)

    # do SummaryPage here too, just to get some coverage
    if doSummary:
        node.summary_page(myKey2, timeoutSecs=timeoutSecs)

    return parseResult
