import unittest, time, sys
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_rf

class Basic(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        h2o.init()
        
    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()
        

    def s3_default_bucket(self):
        return 'h2o_datasets'

    def test_RF_1000trees(self):
        # NAs cause CM to zero..don't run for now
        ### csvPathnamegz = h2o.find_file('smalldata/hhp_9_17_12.predict.100rows.data.gz')
        s3bucket  = self.s3_default_bucket()  
        s3dataset = 'covtype20x.data.gz'
        s3dataset = 'covtype.data'
        s3dataset = 'covtype200x.data.gz'
        s3dataset = 'covtype50x.data'
        s3dataset = 'covtype100x.data'
        s3dataset = 'covtype.20k.data'

        s3dataset = 'covtype.data'
       
        start = time.time()
        parseResult = h2o_cmd.parseS3File(bucket=s3bucket, filename=s3dataset,timeoutSecs=14800)
        print "Parsing took {0}".format(time.time()-start)
        
        start = time.time()
        rf_train = h2o_cmd.runRF(parseResult=parseResult, ntree=100, timeoutSecs=14800, bin_limit=20000, out_of_bag_error_estimate=1,stat_type='ENTROPY',depth=100,exclusive_split_limit=0)
        print "Computation took {0} sec".format(time.time()-start)
        print h2o_rf.pp_rf_result(rf_train)

if __name__ == '__main__':
    h2o.unit_main()
