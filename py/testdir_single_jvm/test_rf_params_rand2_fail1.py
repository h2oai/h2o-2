import unittest, random, sys, time
sys.path.extend(['.','..','py'])
import h2o_browse as h2b, h2o_import2 as h2i
import h2o, h2o_cmd, h2o_rf, h2o_hosts

print "Temporarily not using bin_limit=1 to 4"

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED, localhost
        SEED = h2o.setup_random_seed()
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(node_count=1, java_heap_GB=10)
        else:
            h2o_hosts.build_cloud_with_hosts(node_count=1, java_heap_GB=10)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_rf_params_rand2(self):
        csvPathname = 'UCI/UCI-large/covtype/covtype.data'
        kwargs = {
            'response_variable': 54, 
            'features': 7, 
            'sampling_strategy': 'STRATIFIED_LOCAL', 
            'out_of_bag_error_estimate': 1, 
            'strata_samples': '1=10,2=99,3=99,4=99,5=99,6=99,7=99', 
            'bin_limit': None, 
            'seed': '11111', 
            'model_key': '012345', 
            'ntree': 13, 
            'parallel': 1
        }
        for trial in range(2):

            # adjust timeoutSecs with the number of trees
            timeoutSecs = 30 + ((kwargs['ntree']*20) * max(1,kwargs['features']/15) * (kwargs['parallel'] and 1 or 3))
            start = time.time()
            parseResult = h2i.import_parse(bucket='datasets', path=csvPathname, schema='put')
            rfv = h2o_cmd.runRF(parseResult=parseResult, 
                timeoutSecs=timeoutSecs, retryDelaySecs=1, csvPathname=csvPathname, **kwargs)
            elapsed = time.time()-start

            cm = rfv['confusion_matrix']
            classification_error = cm['classification_error']
            rows_skipped = cm['rows_skipped']

            # just want to catch the nan case when all rows are skipped
            self.assertLess(rows_skipped, 581012)
            self.assertLess(classification_error, 100) # error if nan
            print "Trial #", trial, "completed in", elapsed, "seconds.", \
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)

if __name__ == '__main__':
    h2o.unit_main()
