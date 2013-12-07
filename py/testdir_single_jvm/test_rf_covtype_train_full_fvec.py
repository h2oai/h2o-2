import unittest, random, sys, time
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_rf as h2f, h2o_hosts, h2o_import as h2i, h2o_rf

# we can pass ntree thru kwargs if we don't use the "trees" parameter in runRF
# only classes 1-7 in the 55th col
paramDict = {
    'response': 'A55',
    'ntrees': 30,
    'destination_key': 'model_keyA',
    'max_depth': 20,
    'nbins': 100,
    'ignored_cols_by_name': "A1,A2,A6,A7,A8",
    'sample_rate': 0.80,
    'validation': 'train.csv.hex'
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
        h2o.beta_features = True
        csvFilename = 'train.csv'
        csvPathname = 'bench/covtype/h2o/' + csvFilename
        parseResult = h2i.import_parse(bucket='datasets', path=csvPathname, schema='put', hex_key=csvFilename + ".hex", 
            header=1, timeoutSecs=180)

        for trial in range(1):
            # params is mutable. This is default.
            kwargs = paramDict
            # adjust timeoutSecs with the number of trees
            # seems ec2 can be really slow
            timeoutSecs = 30 + kwargs['ntrees'] * 20
            start = time.time()
            print "Note train.csv is used for both train and validation"
            rfView = h2o_cmd.runRF(parseResult=parseResult, timeoutSecs=timeoutSecs, **kwargs)
            elapsed = time.time() - start
            print "RF end on ", csvPathname, 'took', elapsed, 'seconds.', \
                "%d pct. of timeout" % ((elapsed/timeoutSecs) * 100)
            (classification_error, classErrorPctList, totalScores) = h2o_rf.simpleCheckRFView(rfv=rfView)
            self.assertLess(classification_error, 0.02, "train.csv should have full classification error <0.02")

            print "Trial #", trial, "completed"

if __name__ == '__main__':
    h2o.unit_main()
