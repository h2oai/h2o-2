import unittest, time, sys, random
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_import as h2i, h2o_glm, h2o_exec as h2e

MINFILES = 10
MINDONE = 1

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.init(java_heap_GB=20)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_parse_cust(self):
        # run as user 0xcustomer to get access (with .json config and ssh key file specified)
        importFolderPath = '/mnt/0xcustomer-datasets'
        pollTimeoutSecs = 120
        retryDelaySecs = 30
        timeoutSecs = 300
        
        (importResult, importPattern) = h2i.import_only(path=importFolderPath + "/*")
        importFileList = importResult['files']
        importFailList = importResult['fails']
        importKeyList = importResult['keys']
        importDelList = importResult['dels']

        if len(importDelList)!=0:
            raise Exception("import shouldn't have any deletes. importDelList: %s" % h2o.dump_json(importDelList))

        if len(importFileList)<MINFILES:
            raise Exception("Didn't import successfully. importFileList: %s" % h2o.dump_json(importFileList))

        if len(importKeyList)<MINFILES:
            raise Exception("Didn't import successfully. importKeyList: %s" % h2o.dump_json(importKeyList))

        if len(importFailList)!=0:
            raise Exception("Didn't import successfully. importFailList: %s" % h2o.dump_json(importFailList))


        # only parse files with .csv or .tsv in their name (no dirs like that?)
        goodKeyList = [key for key in importKeyList if ('.csv' in key  or '.tsv' in key)]
        trial = 0
        # just do 1?
        for i, importKey in enumerate(random.sample(goodKeyList,3)):
            print "importKey:", importKey
            trial +=1

            start = time.time() 
            # some data has ,, in the header row. can't have multiple NAs. h2o doesn't like
            # force header=0..should mean headers get treated as NAs
            parseResult = h2i.parse_only(pattern=importKey, header=0,
                timeoutSecs=timeoutSecs, retryDelaySecs=retryDelaySecs, pollTimeoutSecs=pollTimeoutSecs)
            elapsed = time.time() - start
            print "Parse #", trial, "completed in", "%6.2f" % elapsed, "seconds.", \
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)
            print "Parse result['destination_key']:", parseResult['destination_key']

            origKey = parseResult['destination_key']
            inspect = h2o_cmd.runInspect(key=origKey)
            h2o_cmd.infoFromInspect(inspect, origKey)

            execExpr = 'newKey = '+origKey+'[1,1]'
            h2e.exec_expr(h2o.nodes[0], execExpr, "a", timeoutSecs=30)
            newParseKey = {'destination_key': 'newKey'}

            h2o_cmd.checkKeyDistribution()
            h2o.nodes[0].remove_key(key=origKey)
            # a key isn't created for a scalar
            # h2o.nodes[0].remove_key(key='newKey')
        
        self.assertGreater(trial, MINDONE-1, msg="There should be more than %s parsed files" % MINDONE)

if __name__ == '__main__':
    h2o.unit_main()
