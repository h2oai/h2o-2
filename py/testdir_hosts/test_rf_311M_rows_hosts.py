import unittest, sys, time
sys.path.extend(['.','..','py'])

import h2o_cmd, h2o, h2o_hosts
import h2o_browse as h2b, h2o_import as h2i

# Uses your username specific json: pytest_config-<username>.json
# copy pytest_config-simple.json and modify to your needs.
class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_RF_poker_311M(self):
        # since we'll be waiting, pop a browser
        h2b.browseTheCloud()

        importFolderPath = '/home/0xdiag/datasets'
        h2i.setupImportFolder(None, importFolderPath)

        csvFilename = 'new-poker-hand.full.311M.txt.gz'
        for trials in range(2):
            parseKey = h2i.parseImportFolderFile(None, csvFilename, importFolderPath, timeoutSecs=500)
            print csvFilename, 'parse time:', parseKey['response']['time']
            print "Parse result['destination_key']:", parseKey['destination_key']
            inspect = h2o_cmd.runInspect(None,parseKey['destination_key'])

            print "\n" + csvFilename
            start = time.time()
            RFview = h2o_cmd.runRFOnly(trees=5,depth=5,parseKey=parseKey, 
                timeoutSecs=600, retryDelaySecs=10.0)
            print "RF end on ", csvFilename, 'took', time.time() - start, 'seconds'

if __name__ == '__main__':
    h2o.unit_main()


