import unittest, sys, random, time
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i, h2o_hosts

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        pass
        print "Will build clouds with incrementing heap sizes and import folder/parse"

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_import_covtype_parse_loop(self):
        csvFilename = "covtype.data"
        importFolderPath = "/home/0xdiag/datasets"
        trialMax = 2
        for tryHeap in [4,3,2,1]:
            print "\n", tryHeap,"GB heap, 4 jvms, import folder, then loop parsing 'covtype.data' to unique keys"
            localhost = h2o.decide_if_localhost()
            if (localhost):
                h2o.build_cloud(node_count=4,java_heap_GB=tryHeap)
            else:
                h2o_hosts.build_cloud_with_hosts(node_count=4,java_heap_GB=tryHeap)

            h2i.setupImportFolder(None, importFolderPath)
            for trial in range(trialMax):
                key2 = csvFilename + "_" + str(trial) + ".hex"
                parseKey = h2i.parseImportFolderFile(None, csvFilename, importFolderPath, key2=key2, timeoutSecs=20)
            # sticky ports?
            h2o.tear_down_cloud()
            time.sleep(5)

        print "Waiting 60 secs for TIME_WAIT sockets to go away"
        time.sleep(60)



if __name__ == '__main__':
    h2o.unit_main()
