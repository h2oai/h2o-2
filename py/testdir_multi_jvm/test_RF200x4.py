import os, json, unittest, time, shutil, sys
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_hosts

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(4)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_RFhhp(self):
        csvPathnamegz = h2o.find_file('smalldata/hhp.cut3.214.data.gz')

        if not os.path.exists(csvPathnamegz):
            raise Exception("Can't find %s.gz" % (csvPathnamegz))

        print "RF start on ", csvPathnamegz, "this will probably take 1 minute.."
        start = time.time()
        h2o_cmd.runRF(csvPathname=csvPathnamegz, trees=200,
                timeoutSecs=400, retryDelaySecs=15)
        print "RF end on ", csvPathnamegz, 'took', time.time() - start, 'seconds'

if __name__ == '__main__':
    h2o.unit_main()
