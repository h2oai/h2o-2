import unittest
import random, sys
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_rf, h2o_hosts

# we can pass ntree thru kwargs if we don't use the "trees" parameter in runRF
# only classes 1-7 in the 55th col
# don't allow None on ntree..causes 50 tree default!
print "Temporarily not using bin_limit=1 to 4"
print "Temporarily not using stratify"
paramDict = {
    'response_variable': [None,54],
    'class_weight': [None,'1=2','2=2','3=2','4=2','5=2','6=2','7=2'],
    'ntree': [1,3,7,19],
    'model_key': ['model_keyA', '012345', '__hello'],
    'out_of_bag_error_estimate': [None,0,1],
    'gini': [None, 0, 1],
    'depth': [None, 1,10,20,100],
    'bin_limit': [None,5,10,100,1000],
    'parallel': [1],
    'ignore': [None,0,1,2,3,4,5,6,7,8,9],
    'sample': [None,20,40,60,80,90],
    'seed': [None,'0','1','11111','19823134','1231231'],
    'features': [1,3,5,7,9,11,13,17,19,23,37,53],
    'exclusive_split_limit': [None,0,3,5],
    }

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
        h2o.tear_down_cloud()

    def test_loop_random_param_covtype(self):
        csvPathname = h2o.find_dataset('UCI/UCI-large/covtype/covtype.data')

        # for determinism, I guess we should spit out the seed?
        # random.seed(SEED)
        SEED = random.randint(0, sys.maxint)
        # if you have to force to redo a test
        # SEED = 
        random.seed(SEED)
        print "\nUsing random seed:", SEED
        for trial in range(10):
            # params is mutable. This is default.
            params= {'ntree': 23, 'parallel': 1, 'features': 7}
            colX = h2o_rf.pickRandRfParams(paramDict, params)
            kwargs = params.copy()
            print kwargs
            # adjust timeoutSecs with the number of trees
            # seems ec2 can be really slow
            timeoutSecs = 30 + ((kwargs['ntree']*20) * max(1,kwargs['features']/15) * (kwargs['parallel'] and 1 or 3))
            h2o_cmd.runRF(timeoutSecs=timeoutSecs, csvPathname=csvPathname, **kwargs)
            print "Trial #", trial, "completed"

if __name__ == '__main__':
    h2o.unit_main()
