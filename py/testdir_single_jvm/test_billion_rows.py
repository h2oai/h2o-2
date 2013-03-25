import os, json, unittest, time, shutil, sys
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts, h2o_glm
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

    def test_B_importFolder_files(self):
        # just do the import folder once
        # importFolderPath = "/home/hduser/hdfs_datasets"
        importFolderPath = "/home/0xdiag/datasets"
        h2i.setupImportFolder(None, importFolderPath)
        timeoutSecs = 500

        #    "covtype169x.data",
        #    "covtype.13x.shuffle.data",
        #    "3G_poker_shuffle"
        #    "billion_rows.csv.gz",
        csvFilenameAll = [
            # quick test first
            "covtype.data", 
            # then the real thing
            "billion_rows.csv.gz",
            ]
        # csvFilenameList = random.sample(csvFilenameAll,1)
        csvFilenameList = csvFilenameAll

        # pop open a browser on the cloud
        h2b.browseTheCloud()

        for csvFilename in csvFilenameList:
            # creates csvFilename.hex from file in importFolder dir 
            parseKey = h2i.parseImportFolderFile(None, csvFilename, importFolderPath, 
                timeoutSecs=500, pollTimeoutSecs=60)
            print csvFilename, 'parse time:', parseKey['response']['time']
            print "Parse result['destination_key']:", parseKey['destination_key']

            # We should be able to see the parse result?
            inspect = h2o_cmd.runInspect(key=parseKey['destination_key'])

            print "\n" + csvFilename
            start = time.time()
            # poker and the water.UDP.set3(UDP.java) fail issue..
            # constrain depth to 25

            # RF seems to get memory allocation errors on single machine (16GB dram)
            ### RFview = h2o_cmd.runRFOnly(trees=1,depth=5,parseKey=parseKey, timeoutSecs=timeoutSecs)
            ### h2b.browseJsonHistoryAsUrlLastMatch("RFView")

            # now some GLm
            kwargs = {'x': 0, 'y': 1, 'num_cross_validation_folds': 0, 'case_mode': '=', 'case': 1}
            # one coefficient is checked a little more
            colX = 0

            # L2 
            kwargs.update({'alpha': 0, 'lambda': 0})
            start = time.time()
            glm = h2o_cmd.runGLMOnly(parseKey=parseKey, timeoutSecs=timeoutSecs, **kwargs)
            elapsed = time.time() - start
            print "glm (L2) end on ", csvFilename, 'took', elapsed, 'seconds.', "%d pct. of timeout" % ((elapsed/timeoutSecs) * 100)
            h2o_glm.simpleCheckGLM(self, glm, colX, **kwargs)

            sys.stdout.write('\n.')
            sys.stdout.flush() 

if __name__ == '__main__':
    h2o.unit_main()
