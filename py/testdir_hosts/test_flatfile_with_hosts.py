import os, json, unittest, time, shutil, sys
sys.path.extend(['.','..','py'])

import h2o_cmd
import h2o, h2o_hosts

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o_hosts.build_cloud_with_hosts(2,use_flatfile=True)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_A_Basic(self):
        for n in h2o.nodes:
            c = n.get_cloud()
            self.assertEqual(c['cloud_size'], len(h2o.nodes), 'inconsistent cloud size')

    def test_B_RF_iris2(self):
        # FIX! will check some results with RFview
        RFview = h2o_cmd.runRF(trees=6, timeoutSecs=10,
                csvPathname=h2o.find_file('smalldata/iris/iris2.csv'))

    def test_C_RF_poker100(self):
        h2o_cmd.runRF(trees=6, timeoutSecs=10,
                csvPathname=h2o.find_file('smalldata/poker/poker100'))

    def test_D_GenParity1(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        # always match the run below!
        for x in xrange (11,100,10):
            # Have to split the string out to list for pipe
            shCmdString = "perl " + h2o.find_file("syn_scripts/parity.pl") + " 128 4 "+ str(x) + " quad"
            h2o.spawn_cmd_and_wait('parity.pl', shCmdString.split(),timeout=30)
            # the algorithm for creating the path and filename is hardwired in parity.pl..i.e
            csvFilename = "parity_128_4_" + str(x) + "_quad.data"  

        trees = 6
        timeoutSecs = 20
        # always match the gen above!
        # reduce to get intermittent failures to lessen, for now
        for x in xrange (11,60,10):
            sys.stdout.write('.')
            sys.stdout.flush()
            csvFilename = "parity_128_4_" + str(x) + "_quad.data"  
            csvPathname = SYNDATASETS_DIR + '/' + csvFilename
            h2o_cmd.runRF( trees=trees, timeoutSecs=timeoutSecs, csvPathname=csvPathname)
            trees += 10

if __name__ == '__main__':
    h2o.unit_main()
