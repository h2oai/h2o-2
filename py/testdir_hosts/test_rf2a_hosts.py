import os, json, unittest, time, shutil, sys
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_hosts

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_RFhhp(self):
        csvPathnamegz = h2o.find_file('smalldata/hhp_107_01.data.gz')
        print "\nRF start on ", csvPathnamegz, "this will probably take a minute.."
        start = time.time()
        kwargs = {
            'class_weights': '0=1,1=10',
        }

        h2o_cmd.runRF(csvPathname=csvPathnamegz, trees=100,
                timeoutSecs=120, retryDelaySecs=10, **kwargs)
        print "RF end on ", csvPathnamegz, 'took', time.time() - start, 'seconds'

if __name__ == '__main__':
    h2o.unit_main()
