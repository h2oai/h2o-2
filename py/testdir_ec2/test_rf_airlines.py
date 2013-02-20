import os, json, unittest, time, shutil, sys
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_hosts, h2o_pp

class Basic(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        h2o_hosts.build_cloud_with_hosts()
        
    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()
        

    def s3_default_bucket(self):
        return 'h2o_airlines_unpacked'

    def test_RF_1000trees(self):
        s3bucket  = self.s3_default_bucket()  
        s3dataset = 'year1987.csv'
        ignored_cols = 'ArrDelay,DepDelay'
       
        start = time.time()
        parseKey = h2o_cmd.parseS3File(bucket=s3bucket, filename=s3dataset,timeoutSecs=14800, header=True)
        print "Parsing took {0}".format(time.time()-start)
        
        start = time.time()
        rf = h2o_cmd.runRFOnly(parseKey=parseKey, ntree=100, timeoutSecs=14800, bin_limit=20000, out_of_bag_error_estimate=1,gini=0,depth=100,exclusive_split_limit=0,ignore=ignored_cols)
        print "Computation took {0} sec".format(time.time()-start)
        print h2o_pp.pp_rf_result(rf)

if __name__ == '__main__':
    h2o.unit_main()
