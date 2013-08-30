import unittest, time, sys
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts, h2o_import2 as h2i

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(3)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_rf_hhp_2a(self):
        csvFilenameList = {
            'hhp_9_17_12.predict.data.gz',
            'hhp.cut3.214.data.gz',
            # 'hhp_9_17_12.predict.100rows.data.gz',
            }

        print "\nTemporarily won't run some because NAs cause CM=0"
        for csvFilename in csvFilenameList:
            csvPathname = csvFilename
            print "RF start on ", csvPathname, "this will probably take a minute.."
            start = time.time()
            parseResult = h2i.import_parse(bucket='smalldata', path=csvPathname, schema='put')            
            h2o_cmd.runRFOnly(parseResult=parseResult, trees=50, timeoutSecs=300, retryDelaySecs=10)
            print "RF end on ", csvPathname, 'took', time.time() - start, 'seconds'

if __name__ == '__main__':
    h2o.unit_main()
