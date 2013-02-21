import os, json, unittest, time, shutil, sys
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.build_cloud(node_count=3)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_RFhhp(self):
        csvFilenameList = {
            'hhp_9_17_12.predict.data.gz',
            'hhp.cut3.214.data.gz',
            # 'hhp_9_17_12.predict.100rows.data.gz',
            }

        print "\nTemporarily won't run some because NAs cause CM=0"
        for csvFilename in csvFilenameList:
            csvPathname = h2o.find_file('smalldata/' + csvFilename)
            print "RF start on ", csvPathname, "this will probably take a minute.."
            start = time.time()
            h2o_cmd.runRF(csvPathname=csvPathname, trees=6,
                    timeoutSecs=120, retryDelaySecs=10)
            print "RF end on ", csvPathname, 'took', time.time() - start, 'seconds'

if __name__ == '__main__':
    h2o.unit_main()
