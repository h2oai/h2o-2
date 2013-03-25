import os, json, unittest, time, shutil, sys
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts
import h2o_browse as h2b
import h2o_import as h2i
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

    def test_putfile_a5m(self):
        timeoutSecs = 500
        csvFilenameList = [
            # use different names for each parse 
            # doesn't fail if gzipped?
            ("a5m.csv", 'A', None),
            ("a5m.csv", 'B', None),
            ("a5m.csv", 'C', None),
            ]
        # pop open a browser on the cloud
        h2b.browseTheCloud()

        for (csvFilename, key, trees) in csvFilenameList:
            csvPathname = h2o.find_dataset(csvFilename)

            # creates csvFilename and csvFilename.hex  keys
            parseKey = h2o_cmd.parseFile(csvPathname=csvPathname, key=key, timeoutSecs=500)
            print csvFilename, 'parse time:', parseKey['response']['time']
            print "Parse result['destination_key']:", parseKey['destination_key']
            inspect = h2o_cmd.runInspect(key=parseKey['destination_key'])

            print "\n" + csvFilename
            start = time.time()
            # constrain depth to 25
            if trees is not None:
                RFview = h2o_cmd.runRFOnly(trees=trees,depth=25,parseKey=parseKey,
                    timeoutSecs=timeoutSecs)

            h2b.browseJsonHistoryAsUrlLastMatch("RFView")
            # wait in case it recomputes it
            time.sleep(10)

            sys.stdout.write('.')
            sys.stdout.flush() 

if __name__ == '__main__':
    h2o.unit_main()
