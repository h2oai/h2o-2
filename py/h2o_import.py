import h2o, h2o_cmd

def setupImportS3(node=None, path='test-s3-integration'):
    if not node: node = h2o.nodes[0]
    importS3Result = node.import_s3(importS3Path)
    h2o.dump_json(importS3Result)
    return importS3Result

# assumes you call setupImportS3 first
def parseImportS3File(node=None, 
    csvFilename='covtype.data', path='test-s3-integration', key2=None,
    timeoutSecs=None, retryDelaySecs=None):

    if not node: node = h2o.nodes[0]
    if not csvFilename: raise Exception('parseImportS3File: No csvFilename')

    csvPathnameForH2O = "s3:/" + importS3Path + "/" + csvFilename

    # We like the short parse key2 name. 
    # We don't drop anything from csvFilename, unlike H2O default
    if key2 is None:
        # don't rely on h2o default key name
        myKey2 = csvFilename + '.hex'
    else:
        myKey2 = key2

    print "Waiting for the slow parse of the file:", csvFilename
    parseKey = node.parse(csvPathnameForH2O, myKey2, timeoutSecs, retryDelaySecs)
    print "\nParse result:", parseKey
    return parseKey

def setupImportFolder(node=None, path='/home/0xdiag/datasets'):
    if not node: node = h2o.nodes[0]
    importFolderResult = node.import_files(importFolderPath)
    h2o.dump_json(importFolderResult)
    return importFolderResult

# assumes you call setupImportFolder first
def parseImportFolderFile(node=None, csvFilename=None, path=None, key2=None,
    timeoutSecs=None, retryDelaySecs=None, initialDelaySecs=None):
    if not node: node = h2o.nodes[0]
    if not csvFilename: raise Exception('parseImportFolderFile: No csvFilename')

    csvPathnameForH2O = "nfs:/" + importFolderPath + "/" + csvFilename

    # We like the short parse key2 name. 
    # We don't drop anything from csvFilename, unlike H2O default
    if key2 is None:
        # don't rely on h2o default key name
        myKey2 = csvFilename + '.hex'
    else:
        myKey2 = key2

    print "Waiting for the slow parse of the file:", csvFilename
    parseKey = node.parse(csvPathnameForH2O, myKey2, timeoutSecs, retryDelaySecs, initialDelaySecs)
    print "\nParse result:", parseKey
    return parseKey

def setupImportHdfs(node=None, path='hdfs://192.168.1.176/datasets'):
    if not node: node = h2o.nodes[0]
    importHdfsResult = node.import_hdfs(importHdfsPath)
    print h2o.dump_json(importHdfsResult)
    return importHdfsResult

# FIX! can update this to parse from local dir also (import keys from folder?)
# but everyone needs to have a copy then
def parseImportHdfsFile(node=None, csvFilename=None, timeoutSecs=3600, retryDelaySecs=1.0):
    if not csvFilename: raise Exception('No csvFilename parameter in parseImportHdfsFile')
    if not node: node = h2o.nodes[0]

    # assume the hdfs prefix is datasets, for now
    print "\nHacked the test to match the new behavior for key names created from hdfs files"
    
    # FIX! this is ugly..needs to change to use the name node from the config json/h2o args?
    # also the hdfs dir
    hdfsPrefix = "hdfs:hdfs://192.168.1.176/datasets/"
    hdfsKey = hdfsPrefix + csvFilename
    print "parseHdfsFile hdfsKey:", hdfsKey

    # FIX! getting H2O HPE?
    # inspect = node.inspect(hdfsKey)
    inspect = h2o_cmd.runInspect(key=hdfsKey)
    print "parseHdfsFile:", inspect

    parseKey = node.parse(hdfsKey, csvFilename + ".hex", timeoutSecs, retryDelaySecs)
    print "parseHdfsFile:", parseKey
    return parseKey
