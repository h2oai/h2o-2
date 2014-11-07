import unittest, random, sys, time
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i, h2o_glm, h2o_util, h2o_jobs, h2o_gbm, h2o_exec as h2e

DO_BUG = False
DO_HDFS = False
DO_ALL_DIGITS = False
class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        # assume we're at 0xdata with it's hdfs namenode
        h2o.init(1)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_GLM2_mnist(self):
        if DO_HDFS:
            importFolderPath = "mnist"
            bucket = None
            schema = 'hdfs'
        else:
            importFolderPath = "mnist"
            bucket = 'home-0xdiag-datasets'
            schema = 'local'

        csvFilelist = [
            ("mnist_training.csv.gz", "mnist_testing.csv.gz",    600), 
        ]

        trial = 0
        for (trainCsvFilename, testCsvFilename, timeoutSecs) in csvFilelist:
            trialStart = time.time()

            # PARSE test****************************************
            testKey = testCsvFilename + "_" + str(trial) + ".hex"
            csvPathname = importFolderPath + "/" + testCsvFilename
            start = time.time()

            parseTestResult = h2i.import_parse(bucket=bucket, path=csvPathname, schema=schema, hex_key=testKey, timeoutSecs=timeoutSecs)
            
            elapsed = time.time() - start
            print "parse end on ", testCsvFilename, 'took', elapsed, 'seconds',\
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)
            print "parse result:", parseTestResult['destination_key']

            print "We won't use this pruning of x on test data. See if it prunes the same as the training"
            y = 0 # first column is pixel value
            print "y:"
            ignoreX = h2o_glm.goodXFromColumnInfo(y, key=parseTestResult['destination_key'], timeoutSecs=300, returnIgnoreX=True)

            # PARSE train****************************************
            trainKey = trainCsvFilename + "_" + str(trial) + ".hex"
            csvPathname = importFolderPath + "/" + testCsvFilename
            start = time.time()
            parseTrainResult = h2i.import_parse(bucket=bucket, path=csvPathname, schema=schema, hex_key=trainKey, timeoutSecs=timeoutSecs)
            elapsed = time.time() - start
            print "parse end on ", trainCsvFilename, 'took', elapsed, 'seconds',\
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)
            print "parse result:", parseTrainResult['destination_key']

            # GLM****************************************
            print "This is the pruned x we'll use"
            ignoreX = h2o_glm.goodXFromColumnInfo(y, key=parseTrainResult['destination_key'], timeoutSecs=300, 
                returnIgnoreX=True)
            print "ignoreX:", ignoreX 

            modelKey = 'GLM_model'
            params = {
                'ignored_cols': ignoreX, 
                'response': 'C' + str(y+1),
                'family': 'binomial',
                'lambda': 0.5,
                'alpha': 1e-4,
                'max_iter': 15,
                ## 'thresholds': 0.5,
                'n_folds': 1,
                'beta_epsilon': 1.0E-4,
                'destination_key': modelKey,
                }

            if DO_ALL_DIGITS:
                cases = [0,1,2,3,4,5,6,7,8,9]
            else:
                cases = [8]

            for c in cases:
                kwargs = params.copy()
                print "Trying binomial with case:", c
                # kwargs['case_val'] = c

                # do the binomial conversion with Exec2, for both training and test (h2o won't work otherwise)
                if DO_BUG:
                    execExpr="A.hex=%s;A.hex[,%s]=(A.hex[,%s]==%s)" % (trainKey, y+1, y+1, c)
                    h2e.exec_expr(execExpr=execExpr, timeoutSecs=30)
                else:
                    execExpr="A.hex=%s" % (trainKey)
                    h2e.exec_expr(execExpr=execExpr, timeoutSecs=30)
                    execExpr="A.hex[,%s]=(A.hex[,%s]==%s)" % (y+1, y+1, c)
                    h2e.exec_expr(execExpr=execExpr, timeoutSecs=30)

                if DO_BUG:
                    execExpr="B.hex=%s;B.hex[,%s]=(B.hex[,%s]==%s)" % (testKey, y+1, y+1, c)
                    h2e.exec_expr(execExpr=execExpr, timeoutSecs=30)
                else:
                    execExpr="B.hex=%s" % (testKey)
                    h2e.exec_expr(execExpr=execExpr, timeoutSecs=30)

                    execExpr="B.hex[,%s]=(B.hex[,%s]==%s)" % (y+1, y+1, c)
                    h2e.exec_expr(execExpr=execExpr, timeoutSecs=30)

                timeoutSecs = 1800
                start = time.time()
                aHack = {'destination_key': 'A.hex'}
                glmFirstResult = h2o_cmd.runGLM(parseResult=aHack, timeoutSecs=timeoutSecs, pollTimeoutSecs=60, 
                    noPoll=True, **kwargs)
                print "\nglmFirstResult:", h2o.dump_json(glmFirstResult)
                job_key = glmFirstResult['job_key']
                h2o_jobs.pollStatsWhileBusy(timeoutSecs=timeoutSecs, pollTimeoutSecs=60, retryDelaySecs=5)

                # double check...how come the model is bogus?
                h2o_jobs.pollWaitJobs()
                glm = h2o.nodes[0].glm_view(_modelKey=modelKey)

                elapsed = time.time() - start
                print "GLM completed in", elapsed, "seconds.", \
                    "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)

                h2o_glm.simpleCheckGLM(self, glm, None, noPrint=True, **kwargs)
                modelKey = glm['glm_model']['_key']

                # This seems wrong..what's the format of the cm?
                cm = glm['glm_model']['submodels'][0]['validation']['_cms'][-1]['_arr']
                print "cm:", cm
                pctWrong = h2o_gbm.pp_cm_summary(cm);
                # self.assertLess(pctWrong, 9,"Should see less than 9% error (class = 4)")

                print "\nTrain\n==========\n"
                print h2o_gbm.pp_cm(cm)

                # Score *******************************
                # this messes up if you use case_mode/case_vale above
                predictKey = 'Predict.hex'
                start = time.time()

                predictResult = h2o_cmd.runPredict(
                    data_key='B.hex',
                    model_key=modelKey,
                    destination_key=predictKey,
                    timeoutSecs=timeoutSecs)

                predictCMResult = h2o.nodes[0].predict_confusion_matrix(
                    actual='B.hex',
                    vactual='C' + str(y+1),
                    predict=predictKey,
                    vpredict='predict',
                    )

                cm = predictCMResult['cm']

                # These will move into the h2o_gbm.py
                pctWrong = h2o_gbm.pp_cm_summary(cm);
                self.assertLess(pctWrong, 9,"Should see less than 9% error (class = 4)")

                print "\nTest\n==========\n"
                print h2o_gbm.pp_cm(cm)


if __name__ == '__main__':
    h2o.unit_main()
