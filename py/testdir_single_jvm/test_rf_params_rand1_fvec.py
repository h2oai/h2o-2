import unittest, random, sys
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_rf, h2o_import as h2i

paramDict = {
    'destination_key': ['model_keyA', '012345', '__hello'],
    'response': [None,10],
    'balance_classes': [None, 0, 1],
    'classification': [None, 0, 1],
    'cols': [None],
    'ignored_cols_by_name': [None,'C1','C2','C3','C4','C5','C6','C7','C8','C9'],
    'importance': [None, 0, 1],
    'max_after_balance_size': [None,1,3,5,7],
    'max_depth': [None, 1,10,20,100],
    'min_rows': [None, 1,10,20,100],
    'mtries': [None,1,2,3],
    'nbins': [None,4,5,10,100,1000],
    'ntrees': [10, 100,120],
    'sample_rate': [None,0.20,0.40,0.60,0.80,0.90],
    'score_each_iteration': [None, 0, 1],
    'seed': [None,'0','1','11111','19823134','1231231'],
    'validation': [None, 'poker1000.hex'],
    }

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = h2o.setup_random_seed()
        h2o.init()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_rf_params_rand1_fvec(self):
        csvPathname = 'poker/poker1000'
        params = {'ntrees': 2}
        for trial in range(10):
            colX = h2o_rf.pickRandRfParams(paramDict, params)
            kwargs = params.copy()
            # adjust timeoutSecs with the number of trees
            print kwargs
            # slower if parallel=0
            timeoutSecs = 30 + kwargs['ntrees'] * 6
            parseResult = h2i.import_parse(bucket='smalldata', path=csvPathname, hex_key='poker1000.hex', schema='put', 
                timeoutSecs=timeoutSecs)
            h2o_cmd.runRF(parseResult=parseResult, timeoutSecs=timeoutSecs, **kwargs)
            print "Trial #", trial, "completed"

if __name__ == '__main__':
    h2o.unit_main()
