import unittest, time, sys
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd,h2o_hosts, h2o_browse as h2b, h2o_import as h2i, h2o_hosts
import time, random

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global localhost
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(1,java_heap_GB=10)
        else:
            h2o_hosts.build_cloud_with_hosts(1,java_heap_GB=10)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_from_import_fvec(self):

        print "Sets h2o.beat_features like -bf at command line"
        print "this will redirect import and parse to the 2 variants"
        h2o.beta_features = True

        importFolderPath = 'standard'
        timeoutSecs = 500
        csvFilenameAll = [
            "covtype.data",
            "covtype20x.data",
            ]
        # csvFilenameList = random.sample(csvFilenameAll,1)
        csvFilenameList = csvFilenameAll

        # pop open a browser on the cloud
        h2b.browseTheCloud()

        for csvFilename in csvFilenameList:
            # creates csvFilename.hex from file in importFolder dir 
            (importResult, importPattern) = h2i.import_only(path=importFolderPath+"/"+csvFilename, timeoutSecs=50)
            parseResult = h2i.import_parse(path=importFolderPath+"/"+csvFilename, schema='put', timeoutSecs=500)
            if not h2o.beta_features:
                print csvFilename, 'parse time:', parseResult['response']['time']
            print "Parse result['destination_key']:", parseResult['destination_key']
            inspect = h2o_cmd.runInspect(key=parseResult['destination_key'], timeoutSecs=30)

            if not h2o.beta_features:
                RFview = h2o_cmd.runRFOnly(trees=1,depth=25,parseResult=parseResult, timeoutSecs=timeoutSecs)

            ## h2b.browseJsonHistoryAsUrlLastMatch("RFView")
            ## time.sleep(10)

            # just to make sure we test this
            # FIX! currently the importResult is empty for fvec
            if 1==0:
                h2i.delete_keys_from_import_result(pattern=csvFilename, importResult=importResult)

            sys.stdout.write('.')
            sys.stdout.flush() 

if __name__ == '__main__':
    h2o.unit_main()
