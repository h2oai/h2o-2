import os, json, unittest, time, shutil, sys
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

    def test_from_import(self):
        # just do the import folder once
        # importFolderPath = "/home/hduser/hdfs_datasets"
        importFolderPath = '/home/0xdiag/datasets'

        timeoutSecs = 500

        #    "covtype169x.data",
        #    "covtype.13x.shuffle.data",
        #    "3G_poker_shuffle"
        #    "covtype20x.data", 
        #    "billion_rows.csv.gz",
        csvFilenameAll = [
            "covtype.data",
            "covtype20x.data",
            # "covtype200x.data",
            # "100million_rows.csv",
            # "200million_rows.csv",
            # "a5m.csv",
            # "a10m.csv",
            # "a100m.csv",
            # "a200m.csv",
            # "a400m.csv",
            # "a600m.csv",
            # "billion_rows.csv.gz",
            # "new-poker-hand.full.311M.txt.gz",
            ]
        # csvFilenameList = random.sample(csvFilenameAll,1)
        csvFilenameList = csvFilenameAll

        # pop open a browser on the cloud
        ### h2b.browseTheCloud()

        for trial in range(3):
            for csvFilename in csvFilenameList:
                h2i.setupImportFolder(None, importFolderPath)
                # creates csvFilename.hex from file in importFolder dir 
                start = time.time()
                parseKey = h2i.parseImportFolderFile(None, csvFilename, importFolderPath, timeoutSecs=500)
                elapsed = time.time() - start
                print csvFilename, "parsed in", elapsed, "seconds.", "%d pct. of timeout" % ((elapsed*100)/timeoutSecs), "\n"
                print csvFilename, 'H2O reports parse time:', parseKey['response']['time']

                # h2o doesn't produce this, but h2o_import.py adds it for us.
                print "Parse result['source_key']:", parseKey['source_key']
                print "Parse result['destination_key']:", parseKey['destination_key']
                print "\n" + csvFilename

                storeView = h2o.nodes[0].store_view()
                ### print "storeView:", h2o.dump_json(storeView)
                print "Removing", parseKey['source_key'], "so we can re-import it"
                removeKeyResult = h2o.nodes[0].remove_key(key=parseKey['source_key'])
                print "removeKeyResult:", h2o.dump_json(removeKeyResult)

                # we could put an override on the error 
                ### print "Trying to remove a key that's not there anymore?", parseKey['source_key']
                ### removeKeyResult = h2o.nodes[0].remove_key(key=parseKey['source_key'])
                ### print "removeKeyResult:", h2o.dump_json(removeKeyResult)

            print "\nTrial", trial, "completed\n"

if __name__ == '__main__':
    h2o.unit_main()
