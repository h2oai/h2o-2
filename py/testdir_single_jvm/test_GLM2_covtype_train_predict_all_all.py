import unittest, random, sys, time
sys.path.extend(['.','..','../..','py'])

import h2o, h2o_cmd, h2o_import as h2i, h2o_exec, h2o_glm, h2o_gbm, h2o_exec as h2e

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.init(java_heap_GB=10)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_GLM2_covtype_train_predict_all_all(self):
        importFolderPath = "standard"
        csvFilename = 'covtype.shuffled.data'
        csvPathname = importFolderPath + "/" + csvFilename
        hex_key = csvFilename + ".hex"

        # Parse and Exec************************************************
        parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathname, schema='put', hex_key=hex_key, timeoutSecs=180)

        execExpr="A.hex=%s" % parseResult['destination_key']
        h2e.exec_expr(execExpr=execExpr, timeoutSecs=30)

        # use exec to change the output col to binary, case_mode/case_val doesn't work if we use predict
        # will have to live with random extract. will create variance
        # class 4 = 1, everything else 0
        y = 54
        execExpr="A.hex[,%s]=(A.hex[,%s]==%s)" % (y+1, y+1, 1) # class 1
        h2e.exec_expr(execExpr=execExpr, timeoutSecs=30)

        inspect = h2o_cmd.runInspect(key="A.hex")
        print "\n" + csvPathname, \
            "    numRows:", "{:,}".format(inspect['numRows']), \
            "    numCols:", "{:,}".format(inspect['numCols'])

        print "Use same data (full) for train and test"
        trainDataKey = "A.hex"
        testDataKey = "A.hex"
        # start at 90% rows + 1
        
        # GLM, predict, CM*******************************************************8
        kwargs = {
            'response': 'C' + str(y+1),
            'max_iter': 20, 
            'n_folds': 0, 
            # 'alpha': 0.1, 
            # 'lambda': 1e-5, 
            'alpha': 0.0,
            'lambda': None,
            'family': 'binomial',
        }
        timeoutSecs = 60

        for trial in range(1):
            # test/train split **********************************************8
            aHack = {'destination_key': trainDataKey}

            # GLM **********************************************8
            start = time.time()
            glm = h2o_cmd.runGLM(parseResult=aHack, timeoutSecs=timeoutSecs, pollTimeoutSecs=180, **kwargs)
            print "glm end on ", parseResult['destination_key'], 'took', time.time() - start, 'seconds'
            h2o_glm.simpleCheckGLM(self, glm, None, **kwargs)

            modelKey = glm['glm_model']['_key']
            submodels = glm['glm_model']['submodels']
            # hackery to make it work when there's just one
            validation = submodels[-1]['validation']
            best_threshold = validation['best_threshold']
            thresholds = validation['thresholds']

            # have to look up the index for the cm, from the thresholds list
            best_index = None
            for i,t in enumerate(thresholds):
                if t == best_threshold:
                    best_index = i
                    break
            cms = validation['_cms']
            cm = cms[best_index]
            trainPctWrong = h2o_gbm.pp_cm_summary(cm['_arr']);

            # Score **********************************************
            predictKey = 'Predict.hex'
            start = time.time()

            predictResult = h2o_cmd.runPredict(
                data_key=testDataKey,
                model_key=modelKey,
                destination_key=predictKey,
                timeoutSecs=timeoutSecs)

            predictCMResult = h2o.nodes[0].predict_confusion_matrix(
                actual=testDataKey,
                vactual='C' + str(y+1),
                predict=predictKey,
                vpredict='predict',
                )

            cm = predictCMResult['cm']

            # These will move into the h2o_gbm.py
            pctWrong = h2o_gbm.pp_cm_summary(cm);
            self.assertEqual(pctWrong, trainPctWrong,"Should see the same error rate on train and predict? (same data set)")

            print "\nTest\n==========\n"
            print h2o_gbm.pp_cm(cm)

            print "Trial #", trial, "completed"

if __name__ == '__main__':
    h2o.unit_main()
