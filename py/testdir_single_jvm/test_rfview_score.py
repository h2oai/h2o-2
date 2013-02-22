import unittest
import random, sys
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_rf

# we can pass ntree thru kwargs if we don't use the "trees" parameter in runRF
# only classes 1-7 in the 55th col
# don't allow None on ntree..causes 50 tree default!
print "Temporarily not using bin_limit=1 to 4"
paramDict = {
    'response_variable': [None,54],
    'class_weights': [None,'1=2','2=2','3=2','4=2','5=2','6=2','7=2'],
    'ntree': [1,3,7,19],
    'model_key': ['model_keyA', '012345', '__hello'],
    'out_of_bag_error_estimate': [None,0,1],
    'gini': [None, 0, 1],
    'depth': [None, 1,10,20,100],
    'bin_limit': [None,5,10,100,1000],
    'parallel': [None,0,1],
    'ignore': [None,0,1,2,3,4,5,6,7,8,9],
    'sample': [None,20,40,60,80,100],
    'seed': [None,'0','1','11111','19823134','1231231'],
    # stack trace if we use more features than legal. dropped or redundanct cols reduce 
    # legal max also.
    'features': [None,1,3,5,7,9,11,13,17,19,23,37,53],
    'exclusive_split_limit': [None,0,3,5],
    # 'stratify': [None,0,1,1,1,1,1,1,1,1,1],
    'strata': [
        None,
        "0:10",
        "1:5",
        "0:7,2:3",
        "0:1,1:1,2:1,3:1,4:1,5:1,6:1,7:1,8:1,9:1",
        "0:100,1:100,2:100,3:100,4:100,5:100,6:100,7:100,8:100,9:100",
        "0:0,1:0,2:0,3:0,4:0,5:0,6:0,7:0,8:0,9:0",
        ]
    }

# 192.168.0.37:54321/RFView.html?data_key=a5m.hex&model_key=__RFModel_81c5063c-e724-4ebe-bfc1-3ac6838bc628&response_variable=1&ntree=50&class_weights=-1%3D1.0%2C0%3D1.0%2C1%3D1.0&out_of_bag_error_estimate=1&no_confusion_matrix=1&clear_confusion_matrix=1


class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.build_cloud(node_count=1)

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
        csvPathname = h2o.find_dataset('UCI/UCI-large/covtype/covtype.data')
        for trial in range(20):
            # params is mutable. This is default.
            params = {'ntree': 13, 'parallel': 1}
            colX = h2o_rf.pickRandRfParams(paramDict, params)
            kwargs = params.copy()
            # adjust timeoutSecs with the number of trees
            # seems ec2 can be really slow
            timeoutSecs = 30 + 15 * (kwargs['parallel'] and 5 or 10)
            rfv = h2o_cmd.runRF(timeoutSecs=timeoutSecs, retryDelaySecs=1, csvPathname=csvPathname, **kwargs)
    
            ### print "rf response:", h2o.dump_json(rfv)

            model_key = rfv['model_key']
            # pop the stuff from kwargs that were passing as params
            kwargs.pop('model_key',None)

            data_key = rfv['data_key']
            kwargs.pop('data_key',None)

            ntree = rfv['ntree']
            kwargs.pop('ntree',None)
            # just redo it
            h2o_cmd.runRFView(None, data_key, model_key, ntree, timeoutSecs, retryDelaySecs=1, **kwargs)

# scoring
# RFView.html?
# data_key=a5m.hex&
# model_key=__RFModel_81c5063c-e724-4ebe-bfc1-3ac6838bc628&
# response_variable=1&
# ntree=50&
# class_weights=-1%3D1.0%2C0%3D1.0%2C1%3D1.0&
# out_of_bag_error_estimate=1&
# no_confusion_matrix=1&
# clear_confusion_matrix=1
            print "Trial #", trial, "completed"

if __name__ == '__main__':
    h2o.unit_main()
