import unittest, random, sys, time
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_rf as h2f, h2o_import as h2i, h2o_rf, h2o_jobs

# we can pass ntree thru kwargs if we don't use the "trees" parameter in runRF
# only classes 1-7 in the 55th col
paramDict = {
    'response': 'C55',
    'ntrees': 20,
    'destination_key': 'model_keyA',
    'max_depth': 20,
    'nbins': 100,
    # 'ignored_cols_by_name': "A1,A2,A6,A7,A8",
    'sample_rate': 0.80,
    'validation': 'covtype.data.hex',
    'balance_classes': 0,
    'importance': 0,
    }

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.init(1, java_heap_GB=14)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_rf_covtype_train_full_fvec(self):
        csvFilename = 'covtype.data'
        csvPathname = 'standard/' + csvFilename
        parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathname, schema='put', hex_key=csvFilename + ".hex", 
            timeoutSecs=180)

        for trial in range(1):
            # params is mutable. This is default.
            kwargs = paramDict
            # adjust timeoutSecs with the number of trees
            # seems ec2 can be really slow
            timeoutSecs = kwargs['ntrees'] * 60
            start = time.time()
            print "Note train.csv is used for both train and validation"
            rfv = h2o_cmd.runRF(parseResult=parseResult, timeoutSecs=timeoutSecs, noPoll=True, **kwargs)
            h2o_jobs.pollStatsWhileBusy(timeoutSecs=timeoutSecs, retryDelaySecs=5)
            elapsed = time.time() - start
            print "RF end on ", csvPathname, 'took', elapsed, 'seconds.', \
                "%d pct. of timeout" % ((elapsed/timeoutSecs) * 100)

            job_key = rfv['job_key']
            model_key = rfv['destination_key']
            rfv = h2o_cmd.runRFView(data_key=parseResult['destination_key'], 
                model_key=model_key, timeoutSecs=timeoutSecs, retryDelaySecs=1)

            (classification_error, classErrorPctList, totalScores) = h2o_rf.simpleCheckRFView(rfv=rfv)
            # hmm..just using defaults above in RF?
            self.assertLess(classification_error, 4.8, "train.csv should have full classification error: %s < 4.8" % classification_error)

            print "Trial #", trial, "completed"

if __name__ == '__main__':
    h2o.unit_main()
