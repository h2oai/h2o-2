import unittest, sys, time
sys.path.extend(['.','..','py'])
import h2o_cmd, h2o, h2o_hosts, h2o_browse as h2b, h2o_import as h2i

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global localhost
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(node_count=1, java_heap_GB=12)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        ### time.sleep(3600)
        h2o.tear_down_cloud()

    def test_rf_kddcup_1999_fvec(self):
        h2o.beta_features = True
        # h2b.browseTheCloud()
        importFolderPath = 'standard'
        csvFilename = 'kddcup_1999.data.gz'
        csvPathname = importFolderPath + "/" + csvFilename

        parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathname, schema='local',
            timeoutSecs=300)
        print "Parse result['destination_key']:", parseResult['destination_key']
        inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])

        for trials in range(3):
            print "\n" + csvFilename, "Trial #", trials
            start = time.time()

            if trials == 0:
                kwargs = {}
            else:
                kwargs = {
                    'response': 'classifier',
                    'ntrees': 1,
                    # 'features': None,
                    'mtry': 7,
                    'sample_rate': 0.67,
                    'nbins': 1024,
                    'max_depth': 2147483647,
                    'seed': 784834182943470027,
                    }
            if trials == 2:
                kwargs['validation'] 

            start = time.time()
            RFview = h2o_cmd.runRF(parseResult=parseResult, ntrees=1, timeoutSecs=300, retryDelaySecs=1.0, **kwargs)
            print "RF end on ", csvFilename, 'took', time.time() - start, 'seconds'

            ### h2b.browseJsonHistoryAsUrlLastMatch("RFView")

if __name__ == '__main__':
    h2o.unit_main()

# histogram of response classes (42nd field)
#      30 buffer_overflow.
#       8 ftp_write.
#      53 guess_passwd.
#      12 imap.
#   12481 ipsweep.
#      21 land.
#       9 loadmodule.
#       7 multihop.
# 1072017 neptune.
#    2316 nmap.
#  972781 normal.
#       3 perl.
#       4 phf.
#     264 pod.
#   10413 portsweep.
#      10 rootkit.
#   15892 satan.
# 2807886 smurf.
#       2 spy.
#     979 teardrop.
#    1020 warezclient.
#      20 warezmaster.
