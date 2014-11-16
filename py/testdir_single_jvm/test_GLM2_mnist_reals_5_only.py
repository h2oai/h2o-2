import unittest
import random, sys, time, re
sys.path.extend(['.','..','../..','py'])

import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i, h2o_glm, h2o_util, h2o_exec as h2e, h2o_gbm
class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        # assume we're at 0xdata with it's hdfs namenode
        h2o.init(java_heap_GB=12)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_GLM2_mnist_reals(self):
        importFolderPath = "mnist"
        csvFilelist = [
            ("mnist_reals_training.csv.gz", "mnist_reals_testing.csv.gz",    600), 
        ]
        trial = 0
        for (trainCsvFilename, testCsvFilename, timeoutSecs) in csvFilelist:
            trialStart = time.time()

            # PARSE test****************************************
            testKey = testCsvFilename + "_" + str(trial) + ".hex"
            start = time.time()
            parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path="mnist/" + testCsvFilename, schema='put',
                hex_key=testKey, timeoutSecs=timeoutSecs)
            elapsed = time.time() - start
            print "parse end on ", testCsvFilename, 'took', elapsed, 'seconds',\
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)
            print "We won't use this pruning of x on test data. See if it prunes the same as the training"
            y = 0 # first column is pixel value
            print "y:"
            x = h2o_glm.goodXFromColumnInfo(y, key=parseResult['destination_key'], timeoutSecs=300)

            # PARSE train****************************************
            trainKey = trainCsvFilename + "_" + str(trial) + ".hex"
            start = time.time()
            parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path="mnist/" + trainCsvFilename, schema='put',
                hex_key=trainKey, timeoutSecs=timeoutSecs)
            elapsed = time.time() - start
            print "parse end on ", trainCsvFilename, 'took', elapsed, 'seconds',\
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)
            print "parse result:", parseResult['destination_key']

            # GLM****************************************
            print "This is the pruned x GLM will use"
            x = h2o_glm.goodXFromColumnInfo(y, key=parseResult['destination_key'], timeoutSecs=300)
            print "x:", x

            modelKey = "mnist"
            params = {
                'response': y,
                'family': 'binomial',
                'lambda': 1.0E-5,
                'alpha': 0.0,
                'max_iter': 10,
                'n_folds': 1,
                'beta_epsilon': 1.0E-4,
                'destination_key': modelKey
                }

            for c in [5]:
                print "Trying binomial with case:", c
                execExpr="A.hex=%s;A.hex[,%s]=(A.hex[,%s]==%s)" % (trainKey, y+1, y+1, c)
                h2e.exec_expr(execExpr=execExpr, timeoutSecs=30)

                kwargs = params.copy()

                timeoutSecs = 1800
                start = time.time()
                aHack = {'destination_key': 'A.hex'}
                glm = h2o_cmd.runGLM(parseResult=aHack, timeoutSecs=timeoutSecs, pollTimeoutSecs=60, **kwargs)
                elapsed = time.time() - start
                print "GLM completed in", elapsed, "seconds.", \
                    "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)

                h2o_glm.simpleCheckGLM(self, glm, None, noPrint=True, **kwargs)

               # Score **********************************************
                execExpr="B.hex=%s;B.hex[,%s]=(B.hex[,%s]==%s)" % (testKey, y+1, y+1, c)
                h2e.exec_expr(execExpr=execExpr, timeoutSecs=30)

                print "Problems with test data having different enums than train? just use train for now"
                predictKey = 'Predict.hex'
                start = time.time()

                predictResult = h2o_cmd.runPredict(
                    data_key="B.hex",
                    model_key=modelKey,
                    destination_key=predictKey,
                    timeoutSecs=timeoutSecs)

                predictCMResult = h2o.nodes[0].predict_confusion_matrix(
                    actual="B.hex",
                    vactual='C' + str(y+1),
                    predict=predictKey,
                    vpredict='predict',
                    )

                cm = predictCMResult['cm']

                # These will move into the h2o_gbm.py
                pctWrong = h2o_gbm.pp_cm_summary(cm);
                # self.assertLess(pctWrong, 8,"Should see less than 7 pct error (class = 4): %s" % pctWrong)

                print "\nTest\n==========\n"
                print h2o_gbm.pp_cm(cm)



if __name__ == '__main__':
    h2o.unit_main()
