import unittest, random, sys, time
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_rf, h2o_hosts, h2o_import as h2i


paramDict = {
    'destination_key': 'model_keyA', 
    'ntrees': 13, 
    'response': 'C55', 
    'mtries': 3, 
    'source': u'covtype.hex', 
    'seed': '1231231', 
}


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

    def test_rf_params_rand2_fvec(self):
        h2o.beta_features = True
        csvPathname = 'standard/covtype.data'
        for trial in range(3):
            kwargs = paramDict.copy()
            timeoutSecs = 180
            start = time.time()
            parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathname, schema='put')
            rfSeed = random.randint(0, sys.maxint)
            kwargs.update({'seed': rfSeed})
        
            h2o_cmd.runRF(parseResult=parseResult, timeoutSecs=timeoutSecs, retryDelaySecs=1, **kwargs)
            elapsed = time.time()-start
            print "Trial #", trial, "completed in", elapsed, "seconds.", "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)

if __name__ == '__main__':
    h2o.unit_main()
