import os, json, unittest, time, shutil, sys
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd

class glm_same_parse(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    
    @classmethod
    def setUpClass(cls):
        # fails with 3
        h2o.build_cloud(3)
    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud(h2o.nodes)
    
    def test_prostate_then_prostate_long_parse(self):
        print "\nput and parse of same file, but both key and key2 are the h2o defaults..always different"
        for trial in range (10):
            start = time.time()
            key = h2o_cmd.parseFile(csvPathname=h2o.find_file("smalldata/logreg/prostate_long.csv.gz"))
            print "trial #", trial, "parse end on ", "prostate_long.csv.gz" , 'took', time.time() - start, 'seconds'
            h2o.check_sandbox_for_errors()

if __name__ == '__main__':
    h2o.unit_main()
