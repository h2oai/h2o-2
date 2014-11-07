import unittest, random, sys, time
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_rf, h2o_import as h2i, h2o_util

paramDict = {
    # 2 new
    'destination_key': ['model_keyA', '012345', '__hello'],
    'cols': [None, None, None, None, None, '0,1,2,3,4,5,6,7,8','C1,C2,C3,C4,C5,C6,C7,C8'],
    # exclusion handled below, otherwise exception:
    # ...Arguments 'cols', 'ignored_cols_by_name', and 'ignored_cols' are exclusive

    'ignored_cols_by_name': [None, None, None, None, 'C1','C2','C3','C4','C5','C6','C7','C8','C9'],
    # probably can't deal with mixtures of cols and ignore, so just use cols for now
    # could handle exclusion below
    # 'ignored_cols': [None, None, None, None, None, '0,1,2,3,4,5,6,7,8','C1,C2,C3,C4,C5,C6,C7,C8'],
    'n_folds': [None, 2, 5], # has to be >= 2?
    'keep_cross_validation_splits': [None, 0, 1],
    # 'classification': [None, 0, 1],
    # doesn't support regression yet
    'classification': [None, 1],
    'balance_classes': [None, 0, 1],
    # never run with unconstrained balance_classes size if random sets balance_classes..too slow
    'max_after_balance_size': [.1, 1, 2],
    'oobee': [None, 0, 1],
    'sampling_strategy': [None, 'RANDOM'],
    'select_stat_type': [None, 'ENTROPY', 'GINI'],
    'response': [54, 'C55'], # equivalent. None is not legal
    'validation': [None, 'covtype.data.hex'],
    'ntrees': [1], # just do one tree
    'importance': [None, 0, 1],
    'max_depth': [None, 1,10,20,100],
    'nbins': [None,5,10,100,1000],
    'sample_rate': [None,0.20,0.40,0.60,0.80,0.90],
    'seed': [None,'0','1','11111','19823134','1231231'],
    # Can't have more mtries than cols..force to 4 if cols is not None?
    'mtries': [1,3,5,7],
    }

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = h2o.setup_random_seed()
        h2o.init(java_heap_GB=10)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_speedrf_params_rand2_fvec(self):
        csvPathname = 'standard/covtype.data'
        hex_key = 'covtype.data.hex'
        for trial in range(10):
            # params is mutable. This is default.
            # response is required for SpeeERF
            params = {
                'response': 'C55', 
                'ntrees': 1, 'mtries': 7, 
                'balance_classes': 0, 
                # never run with unconstrained balance_classes size if random sets balance_classes..too slow
                'max_after_balance_size': 2,
                'importance': 0}
            colX = h2o_util.pickRandParams(paramDict, params)
            if 'cols' in params and params['cols']:
                # exclusion
                if 'ignored_cols_by_name' in params:
                    params['ignored_cols_by_name'] = None
            else:
                if 'ignored_cols_by_name' in params and params['ignored_cols_by_name']:
                    params['mtries'] = random.randint(1,53)
                else:
                    params['mtries'] = random.randint(1,54)
                
            kwargs = params.copy()
            # adjust timeoutSecs with the number of trees
            timeoutSecs = 80 + ((kwargs['ntrees']*80) * max(1,kwargs['mtries']/60) )
            start = time.time()
            parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathname, schema='put', hex_key=hex_key)
            h2o_cmd.runSpeeDRF(parseResult=parseResult, timeoutSecs=timeoutSecs, retryDelaySecs=1, **kwargs)
            elapsed = time.time()-start
            print "Trial #", trial, "completed in", elapsed, "seconds.", "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)

if __name__ == '__main__':
    h2o.unit_main()
