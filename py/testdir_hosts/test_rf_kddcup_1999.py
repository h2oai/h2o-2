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
        h2o_hosts.build_cloud_with_hosts(1)

    @classmethod
    def tearDownClass(cls):
        ### time.sleep(3600)
        h2o.tear_down_cloud()

    def test_rf_kddcup_1999(self):
        # since we'll be waiting, pop a browser
        h2b.browseTheCloud()

        importFolderPath = '/home/0xdiag/datasets'
        h2i.setupImportFolder(None, importFolderPath)
        csvFilename = 'kddcup_1999.data'

        print "Want to see that I get similar results when using H2O RF defaults (no params to json)" +\
            "compared to running with the parameters specified and matching the browser RF query defaults. " +\
            "Also run the param for full scoring vs OOBE scoring."

        parseKey = h2i.parseImportFolderFile(None, csvFilename, importFolderPath, timeoutSecs=300)
        print csvFilename, 'parse time:', parseKey['response']['time']
        print "Parse result['destination_key']:", parseKey['destination_key']
        inspect = h2o_cmd.runInspect(None,parseKey['destination_key'])

        for trials in range(4):
            print "\n" + csvFilename, "Trial #", trials
            start = time.time()

            kwargs = {
                'response_variable': 'classifier',
                'ntree': 200,
                'gini': 1,
                'class_weights': None,
                'stratify': 0,
                # 'features': None,
                'features': 7,
                'ignore': None,
                'sample': 67,
                'bin_limit': 1024,
                'depth': 2147483647,
                'seed': 784834182943470027,
                'parallel': 1,
                'exclusive_split_limit': None,
                }

            if trials == 0:
                kwargs = {}
            elif trials == 1:
                kwargs['out_of_bag_error_estimate'] = None
            elif trials == 2:
                kwargs['out_of_bag_error_estimate'] = 0
            elif trials == 3:
                kwargs['out_of_bag_error_estimate'] = 1

            start = time.time()
            RFview = h2o_cmd.runRFOnly(trees=50,parseKey=parseKey, 
                timeoutSecs=300, retryDelaySecs=1.0, **kwargs)
            print "RF end on ", csvFilename, 'took', time.time() - start, 'seconds'

            h2b.browseJsonHistoryAsUrlLastMatch("RFView")

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
