import unittest, random, sys, time
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i, h2o_glm, h2o_util, h2o_jobs, h2o_gbm, h2o_exec as h2e

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.init(1)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_GLM2_mnist_short(self):
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

            parseTestResult = h2i.import_parse(bucket=bucket, path=csvPathname, schema=schema, 
                hex_key=testKey, timeoutSecs=timeoutSecs, doSummary=False)
            
            elapsed = time.time() - start
            print "parse end on ", testCsvFilename, 'took', elapsed, 'seconds',\
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)
            print "parse result:", parseTestResult['destination_key']

            print "We won't use this pruning of x on test data. See if it prunes the same as the training"
            
            # first col is pixel value ..use 0 here
            y = 0
            ignoreX = h2o_glm.goodXFromColumnInfo(y, key=parseTestResult['destination_key'], timeoutSecs=300, returnIgnoreX=True)

            # PARSE train****************************************
            trainKey = trainCsvFilename + "_" + str(trial) + ".hex"
            csvPathname = importFolderPath + "/" + testCsvFilename
            start = time.time()
            parseTrainResult = h2i.import_parse(bucket=bucket, path=csvPathname, schema=schema, 
                hex_key=trainKey, timeoutSecs=timeoutSecs, doSummary=False)
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
                # first column is pixel value
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

            cases = [8]
            for c in cases:
                kwargs = params.copy()
                print "Trying binomial with case:", c
                # kwargs['case_val'] = c

                # do the binomial conversion with Exec2, for both training and test (h2o won't work otherwise)
                execExpr="A.hex=%s" % (trainKey)
                h2e.exec_expr(execExpr=execExpr, timeoutSecs=30)

                execExpr="A.hex[,%s]=(A.hex[,%s]==%s)" % (y+1, y+1, c)
                h2e.exec_expr(execExpr=execExpr, timeoutSecs=30)
                h2o_cmd.runSummary(key=trainKey, cols=0, max_ncols=1, noPrint=False)
                h2o_cmd.runSummary(key='A.hex', cols=0, max_ncols=1, noPrint=False)

                execExpr="B.hex=%s" % (testKey)
                h2e.exec_expr(execExpr=execExpr, timeoutSecs=30)

                execExpr="B.hex[,%s]=(B.hex[,%s]==%s)" % (y+1, y+1, c)
                h2e.exec_expr(execExpr=execExpr, timeoutSecs=30)
                h2o_cmd.runSummary(key=testKey, cols=0, max_ncols=1, noPrint=False)
                h2o_cmd.runSummary(key='B.hex', cols=0, max_ncols=1, noPrint=False)


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
                print "GLM completed in", elapsed, "seconds.", "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)

                h2o_glm.simpleCheckGLM(self, glm, None, noPrint=True, **kwargs)
                modelKey = glm['glm_model']['_key']

                cm = glm['glm_model']['submodels'][0]['validation']['_cms'][-1]['_arr']
                print "cm:", cm
                pctWrong = h2o_gbm.pp_cm_summary(cm);
                # self.assertLess(pctWrong, 9,"Should see less than 9% error (class = 4)")

                print "\nTrain\n==========\n"
                print h2o_gbm.pp_cm(cm)




if __name__ == '__main__':
    h2o.unit_main()
