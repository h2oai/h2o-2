import unittest, random, sys
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_rf, h2o_hosts, h2o_import as h2i

paramDict = {
    'response': [None,'C10'],
    'ntrees': [10, 100,120],
    'destination_key': ['model_keyA', '012345', '__hello'],
    'max_depth': [None, 1,10,20,100],
    'nbins': [None,4,5,10,100,1000],
    'ignored_cols_by_name': [None, None, None, None, 'C1','C2','C3','C4','C5','C6','C7','C8','C9'],
    # undecided node assertion if input cols don't change
    # 'cols': [None, None, None, None, None, '0,1,2,3,4,','C1,C2,C3,C4'],
    'sample_rate': [None,0.20,0.40,0.60,0.80,0.90],
    'seed': [None,'0','1','11111','19823134','1231231'],
    # I guess rf drops constant columns in poiker1000 so max of 4 features ??
    # 4 is broken because as another test shows, RF won't let me select all the 
    # all the allowed features it says I can. So I'll live with 3 here.
    'mtries': [None,1,2,3],
    # only works on new
    'min_rows': [None,1,3,5],
    'importance': [None,0,1],
    'classification': [None,0,1],
    'validation': [None, 'poker1000.hex'],
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
            h2o.build_cloud(node_count=1)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_rf_params_rand1_fvec(self):
        h2o.beta_features = True
        csvPathname = 'poker/poker1000'
        for trial in range(10):
            # params is mutable. This is default.
            params = {'ntrees': 63}
            colX = h2o_rf.pickRandRfParams(paramDict, params)
            kwargs = params.copy()
            # adjust timeoutSecs with the number of trees
            print kwargs
            # slower if parallel=0
            timeoutSecs = 30 + kwargs['ntrees'] * 30
            parseResult = h2i.import_parse(bucket='smalldata', path=csvPathname, schema='put', timeoutSecs=timeoutSecs)
            h2o_cmd.runRF(parseResult=parseResult, timeoutSecs=timeoutSecs, **kwargs)
            print "Trial #", trial, "completed"

if __name__ == '__main__':
    h2o.unit_main()
