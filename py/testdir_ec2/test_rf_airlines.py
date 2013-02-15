import os, json, unittest, time, shutil, sys
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_hosts

def pp(rf):
    return """
 Leaves: {0} / {1} / {2}
  Depth: {3} / {4} / {5}
   mtry: {6}
    err: {7} %
""".format(rf['trees']['leaves']['min'],rf['trees']['leaves']['mean'],rf['trees']['leaves']['max'],rf['trees']['depth']['min'],rf['trees']['depth']['mean'],rf['trees']['depth']['max'],-1, 0)
class Basic(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        h2o_hosts.build_cloud_with_hosts()
        
    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()
        

    def s3_default_bucket(self):
        return 'h2o_airlines'

    def test_RF_1000trees(self):
        s3bucket  = self.s3_default_bucket()  
        s3dataset = '1987.csv.bz2'
       
        start = time.time()
        parseKey = h2o_cmd.parseS3File(bucket=s3bucket, filename=s3dataset,timeoutSecs=14800)
        print "Parsing took {0}".format(time.time()-start)
        
        start = time.time()
	rf = h2o_cmd.runRFOnly(parseKey=parseKey, ntree=100, timeoutSecs=14800, bin_limit=20000, out_of_bag_error_estimate=1,gini=0,depth=100,exclusive_split_limit=0)
        print "Computation took {0} sec".format(time.time()-start)
        print "Result:",rf
        #print pp(rf)

if __name__ == '__main__':
    h2o.unit_main()
