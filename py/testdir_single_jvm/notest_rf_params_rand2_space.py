import unittest
import random, sys, time
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_rf, h2o_hosts

# we can pass ntree thru kwargs if we don't use the "trees" parameter in runRF
# only classes 1-7 in the 55th col
# don't allow None on ntree..causes 50 tree default!
print "Temporarily not using bin_limit=1 to 4"
paramDict = {
    # 'class_weights': [None,'yes=1000','no=1000'], 'model_key': ['model_keyA', '012345', '__hello'],
    'depth': [None, 1,10,20,100], 'bin_limit': [None,5,10,100,1000],
    'parallel': [None,0,1],
    'seed': [None,'0','1','11111','19823134','1231231'],
    # stack trace if we use more features than legal. dropped or redundanct cols reduce 
    # legal max also.
    'features': [None,1],
    'exclusive_split_limit': [None],
    'sampling_strategy': [None, 'RANDOM', 'STRATIFIED_LOCAL' ],
    'strata_samples': [
        None,
        "no=10",
        "yes=5",
        "no=7,yes=3",
        "yes=1,no=1",
        "no=100,yes=100",
        "no=0,yes=0",
        ]
    }

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global localhost
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(node_count=1)
        else:
            h2o_hosts.build_cloud_with_hosts(node_count=1)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_rf_params_rand2(self):
        # for determinism, I guess we should spit out the seed?
        # random.seed(SEED)
        SEED = random.randint(0, sys.maxint)
        # if you have to force to redo a test
        # SEED = 
        random.seed(SEED)
        print "\nUsing random seed:", SEED
        csvPathname = h2o.find_file('smalldata/space_shuttle_damage.csv')
        for trial in range(10):
            # params is mutable. This is default.
            params = {
                'sample': 80,
                'stat_type': 'ENTROPY',
                'class_weights': 'yes=1000',
                'ntree': 50, 
                'parallel': 1, 
                'response_variable': 'damage', 
                'ignore': 'flight',
                'ntree': 25,
                'out_of_bag_error_estimate': 1,
            }
            print "params:", params 
            colX = h2o_rf.pickRandRfParams(paramDict, params)
            print "params:", params 
            kwargs = params.copy()
            # adjust timeoutSecs with the number of trees
            # seems ec2 can be really slow
            timeoutSecs = 30 + 15 * (kwargs['parallel'] and 6 or 10)
            start = time.time()
            rfView = h2o_cmd.runRF(timeoutSecs=timeoutSecs, retryDelaySecs=1, csvPathname=csvPathname, **kwargs)
            elapsed = time.time()-start
            # just to get the list of per class errors
            (classification_error, classErrorPctList, totalScores) = h2o_rf.simpleCheckRFView(None, rfView, noPrint=True)
            print "Trial #", trial, "completed in", elapsed, "seconds.", "%d pct. of timeout" % ((elapsed*100)/timeoutSecs), "\n"
            # why does this vary between 22 and 23
            self.assertAlmostEqual(totalScores,23,delta=1) # class 1 is 'yes'
            self.assertLess(classErrorPctList[0],95) # class 0 is 'no'
            self.assertLess(classErrorPctList[1],29) # class 1 is 'yes'
            self.assertLess(classification_error,61)

if __name__ == '__main__':
    h2o.unit_main()
