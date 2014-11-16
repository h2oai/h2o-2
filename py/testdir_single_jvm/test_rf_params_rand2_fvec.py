import unittest, random, sys, time
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_rf, h2o_import as h2i

# we can pass ntree thru kwargs if we don't use the "trees" parameter in runRF
# only classes 1-7 in the 55th col
# don't allow None on ntree..causes 50 tree default!
print "Temporarily not using bin_limit=1 to 4"
paramDict = {
    # 2 new
    'score_each_iteration': [None, None, None, 0, 1],
    'response': [None, 'C55'],
    'validation': [None, 'covtype.data.hex'],
    'ntrees': [1,3,7,19],
    'importance': [None, 0, 1],
    'balance_classes': [0],
    'destination_key': ['model_keyA', '012345', '__hello'],
    'max_depth': [None, 1,10,20,100],
    'nbins': [None,5,10,100,1000],
    'ignored_cols_by_name': [None, None, None, None, 'C1','C2','C3','C4','C5','C6','C7','C8','C9'],
    'cols': [None, None, None, None, None, '0,1,2,3,4,5,6,7,8','C1,C2,C3,C4,C5,C6,C7,C8'],

    'sample_rate': [None,0.20,0.40,0.60,0.80,0.90],
    'seed': [None,'0','1','11111','19823134','1231231'],
    # stack trace if we use more features than legal. dropped or redundanct cols reduce 
    # legal max also.
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

    def test_rf_params_rand2_fvec(self):
        csvPathname = 'standard/covtype.data'
        hex_key = 'covtype.data.hex'
        for trial in range(2):
            # params is mutable. This is default.
            params = {'ntrees': 13, 'mtries': 7, 'balance_classes': 0, 'importance': 0}
            colX = h2o_rf.pickRandRfParams(paramDict, params)
            if 'cols' in params and params['cols']:
                pass
            else:
                if 'ignored_cols_by_name' in params and params['ignored_cols_by_name']:
                    params['mtries'] = random.randint(1,53)
                else:
                    params['mtries'] = random.randint(1,54)
                
            kwargs = params.copy()
            # adjust timeoutSecs with the number of trees
            timeoutSecs = 30 + ((kwargs['ntrees']*80) * max(1,kwargs['mtries']/60) )
            start = time.time()
            parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathname, schema='put', hex_key=hex_key)
            h2o_cmd.runRF(parseResult=parseResult, timeoutSecs=timeoutSecs, retryDelaySecs=1, **kwargs)
            elapsed = time.time()-start
            print "Trial #", trial, "completed in", elapsed, "seconds.", "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)

if __name__ == '__main__':
    h2o.unit_main()
