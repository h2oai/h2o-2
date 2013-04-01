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

    def test_B_putfile_files(self):
        timeoutSecs = 500

        #    "covtype169x.data",
        #    "covtype.13x.shuffle.data",
        #    "3G_poker_shuffle"
        #    "covtype20x.data", 
        #    "billion_rows.csv.gz",
        csvFilenameList = [
            ("covtype.data", 'UCI/UCI-large/covtype/covtype.data', 1),
            ]
        # pop open a browser on the cloud
        h2b.browseTheCloud()

        for (csvFilename, datasetPath, trees) in csvFilenameList:
            csvPathname = h2o.find_dataset(datasetPath)

            # creates csvFilename and csvFilename.hex  keys
            node = h2o.nodes[0]
            key = node.put_file(csvPathname, key=csvFilename, timeoutSecs=timeoutSecs)
            # not using parseFile...used to be a bug if we inspect the file we just put
            # so we test that
            inspect1 = h2o_cmd.runInspect(key=csvFilename)

            parseKey = node.parse(key, timeoutSecs=500)
            print csvFilename, 'parse time:', parseKey['response']['time']
            print "Parse result['destination_key']:", parseKey['destination_key']
            # We should be able to see the parse result?
            inspect2 = h2o_cmd.runInspect(key=parseKey['destination_key'])

            print "\n" + csvFilename
            start = time.time()
            # constrain depth to 25
            if trees is not None:
                RFview = h2o_cmd.runRFOnly(trees=trees,depth=25,parseKey=parseKey,
                    timeoutSecs=timeoutSecs)

            sys.stdout.write('.')
            sys.stdout.flush() 

if __name__ == '__main__':
    h2o.unit_main()
