import unittest, sys, time
sys.path.extend(['.','..','../..','py'])
import h2o_cmd, h2o, h2o_browse as h2b, h2o_import as h2i

# Uses your username specific json: pytest_config-<username>.json
# copy pytest_config-simple.json and modify to your needs.
class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.init()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_rf_311M_rows_fvec(self):
        # since we'll be waiting, pop a browser
        # h2b.browseTheCloud()
        importFolderPath = 'standard'
        csvFilename = 'new-poker-hand.full.311M.txt.gz'
        csvPathname = importFolderPath + "/" + csvFilename

        for trials in range(1):
            parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathname, schema='local', 
                timeoutSecs=1400, retryDelaySecs=5)
            print "Parse result['destination_key']:", parseResult['destination_key']
            inspect = h2o_cmd.runInspect(None,parseResult['destination_key'])

            print "\n" + csvFilename
            start = time.time()

            kwargs = {
                'ntrees': 1,
                'max_depth': 5,
                'importance': 0,
            }

            RFview = h2o_cmd.runRF(parseResult=parseResult, timeoutSecs=800, retryDelaySecs=20.0, **kwargs)
            print "RF end on ", csvFilename, 'took', time.time() - start, 'seconds'

if __name__ == '__main__':
    h2o.unit_main()


