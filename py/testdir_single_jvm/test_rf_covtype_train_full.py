import unittest
import random, sys, time
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_rf as h2f, h2o_hosts

# we can pass ntree thru kwargs if we don't use the "trees" parameter in runRF
# only classes 1-7 in the 55th col
# rng DETERMINISTIC is default
paramDict = {
    'response_variable': 54,
    'class_weight': None,
    'ntree': 30,
    # 'ntree': 200,
    'model_key': 'model_keyA',
    'out_of_bag_error_estimate': 0,
    'gini': 1,
    'depth': 2147483647, 
    'bin_limit': 10000,
    'parallel': 1,
    'ignore': "1,2,6,7,8",
    'sample': 80,
    ### 'seed': 3,
    ### 'features': 30, 
    'exclusive_split_limit': 0, 
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

    def test_rf_covtype_train_full(self):
        csvFilename = 'train.csv'
        csvPathname = h2o.find_dataset('bench/covtype/h2o/' + csvFilename)
        print "\nUsing header=1 even though I shouldn't have to. Otherwise I get NA in first row and RF bad\n"
        parseKey = h2o_cmd.parseFile(csvPathname=csvPathname, key2=csvFilename + ".hex", header=1, 
            timeoutSecs=180)

        for trial in range(1):
            # params is mutable. This is default.
            kwargs = paramDict
            # adjust timeoutSecs with the number of trees
            # seems ec2 can be really slow
            timeoutSecs = 30 + kwargs['ntree'] * 20
            start = time.time()
            rfView = h2o_cmd.runRF(csvPathname=csvPathname, timeoutSecs=timeoutSecs, **kwargs)
            elapsed = time.time() - start
            print "RF end on ", csvPathname, 'took', elapsed, 'seconds.', \
                "%d pct. of timeout" % ((elapsed/timeoutSecs) * 100)

            classification_error = rfView['confusion_matrix']['classification_error']
            self.assertLess(classification_error, 0.02, 
                "train.csv should have full classification error <0.02")

            print "Trial #", trial, "completed"

if __name__ == '__main__':
    h2o.unit_main()
