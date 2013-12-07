import unittest, random, sys
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_rf, h2o_hosts, h2o_import as h2i

# make a dict of lists, with some legal choices for each. None means no value.
# assume poker1000 datset

# we can pass ntree thru kwargs if we don't use the "trees" parameter in runRF
# only classes 0-8 in the last col of poker1000

# FIX! lots of stratify for now..temporary testing
# FIX! make binLimit minimum of 2 for now? bug with 1?
print "Temporarily not using bin_limit=1 to 3"
print "Temporarily not using 100% sampling with out_of_bag_error_estimate==1"
print "Temporarily only using max of 3 features, rather than the 4 non-constant input columns"
paramDict = {
    'use_non_local_data': [None, 0, 1],
    'iterative_cm': [None, 0, 1],
    'response_variable': [None,10],
    'class_weights': [None,'1=2','2=2','3=2','4=2','5=2','6=2','7=2','8=2'],
    'ntree': [10, 100,120],
    'model_key': ['model_keyA', '012345', '__hello'],
    'out_of_bag_error_estimate': [None,0,1],
    'stat_type': [None, 'ENTROPY', 'GINI'],
    'depth': [None, 1,10,20,100],
    'bin_limit': [None,4,5,10,100,1000],
    'ignore': [None,0,1,2,3,4,5,6,7,8,9],
    'sample': [None,20,40,60,80,90],
    'seed': [None,'0','1','11111','19823134','1231231'],
    # I guess rf drops constant columns in poiker1000 so max of 4 features ??
    # 4 is broken because as another test shows, RF won't let me select all the 
    # all the allowed features it says I can. So I'll live with 3 here.
    'features': [None,1,2,3],
    # only works on new
    'exclusive_split_limit': [None,0,3,5],
    'sampling_strategy': [None, 'RANDOM', 'STRATIFIED_LOCAL' ],
    'strata_samples': [
        None,
        "0=10",
        "1=5",
        "2=3", 
        "1=1,2=1,3=1,4=1,5=1,6=1,7=1,8=1",
        "1=100,2=100,3=100,4=100,5=100,6=100,7=100,8=100",
        "1=0,1=0,2=0,3=0,4=0,5=0,6=0,7=0,8=0",
        ]
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
            h2o_hosts.build_cloud_with_hosts(node_count=1)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_rf_params_rand1(self):
        csvPathname = 'poker/poker1000'
        for trial in range(10):
            # params is mutable. This is default.
            params = {'ntree': 63, 'use_non_local_data': 1}
            colX = h2o_rf.pickRandRfParams(paramDict, params)
            kwargs = params.copy()
            # adjust timeoutSecs with the number of trees
            print kwargs
            # slower if parallel=0
            timeoutSecs = 30 + kwargs['ntree'] * 6
            parseResult = h2i.import_parse(bucket='smalldata', path=csvPathname, schema='put', timeoutSecs=timeoutSecs)
            h2o_cmd.runRF(parseResult=parseResult, timeoutSecs=timeoutSecs, **kwargs)
            print "Trial #", trial, "completed"

if __name__ == '__main__':
    h2o.unit_main()
