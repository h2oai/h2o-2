import unittest
import random, sys, time
sys.path.extend(['.','..','py'])

print "This case failed with all rows_skipped?"
import h2o, h2o_cmd, h2o_rf, h2o_hosts

print "Temporarily not using bin_limit=1 to 4"

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global localhost
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(node_count=1, java_heap_GB=10)
        else:
            h2o_hosts.build_cloud_with_hosts(node_count=1, java_heap_GB=10)

    @classmethod
    def tearDownClass(cls):
        time.sleep(3600)
        h2o.tear_down_cloud()

    def test_rf_params_rand2(self):
        h2b.browseTheCloud()

        # for determinism, I guess we should spit out the seed?
        # random.seed(SEED)
        SEED = random.randint(0, sys.maxint)
        # if you have to force to redo a test
        # SEED = 
        random.seed(SEED)
        print "\nUsing random seed:", SEED
        csvPathname = h2o.find_dataset('UCI/UCI-large/covtype/covtype.data')
        for trial in range(2):
            kwargs = {
                'response_variable': None, 
                'features': 7, 
                'sampling_strategy': 'STRATIFIED_LOCAL', 
                'out_of_bag_error_estimate': 1, 
                'strata_samples': '1=100,2=100,3=100,4=100,5=100,6=100,7=100', 
                'bin_limit': None, 
                'seed': '11111', 
                'model_key': '012345', 
                'ntree': 13, 
                'parallel': 1
            }

            # adjust timeoutSecs with the number of trees
            timeoutSecs = 30 + ((kwargs['ntree']*20) * max(1,kwargs['features']/15) * (kwargs['parallel'] and 1 or 3))
            start = time.time()
            rfView = h2o_cmd.runRF(timeoutSecs=timeoutSecs, retryDelaySecs=1, csvPathname=csvPathname, **kwargs)
            elapsed = time.time()-start

            cm = rfv['confusion_matrix']
            classification_error = cm['classification_error']
            rows_skipped = cm['rows_skipped']

            # just want to catch the nan case when all rows are skipped
            self.assertLessThan(rows_skipped, 581012)
            self.assertLessThan(classification_error, 100) # error if nan
            print "Trial #", trial, "completed in", elapsed, "seconds.", \
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)

if __name__ == '__main__':
    h2o.unit_main()
