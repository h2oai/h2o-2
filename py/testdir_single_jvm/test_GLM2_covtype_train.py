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

    def test_GLM2_covtype_train(self):
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
        execExpr="A.hex[,%s]=(A.hex[,%s]==%s)" % (y+1, y+1, 4)
        h2e.exec_expr(execExpr=execExpr, timeoutSecs=30)

        inspect = h2o_cmd.runInspect(key="A.hex")
        print "\n" + csvPathname, \
            "    numRows:", "{:,}".format(inspect['numRows']), \
            "    numCols:", "{:,}".format(inspect['numCols'])

        # Split Test/Train************************************************
        # how many rows for each pct?
        numRows = inspect['numRows']
        pct10 = int(numRows * .1)
        rowsForPct = [i * pct10 for i in range(0,11)]
        # this can be slightly less than 10%
        last10 = numRows - rowsForPct[9]
        rowsForPct[10] = last10
        # use mod below for picking "rows-to-do" in case we do more than 9 trials
        # use 10 if 0 just to see (we copied 10 to 0 above)
        rowsForPct[0] = rowsForPct[10]

        print "Creating the key of the last 10% data, for scoring"
        trainDataKey = "rTrain"
        testDataKey = "rTest"
        # start at 90% rows + 1
        
        # GLM, predict, CM*******************************************************8
        kwargs = {
            'response': 'C' + str(y+1),
            'max_iter': 20, 
            'n_folds': 0, 
            'alpha': 0.1, 
            'lambda': 1e-5, 
            'family': 'binomial',
            'higher_accuracy': 1,
        }
        timeoutSecs = 180

        for trial in range(10):
            # always slice from the beginning
            rowsToUse = rowsForPct[trial%10] 

            # test/train split **********************************************8
            h2o_cmd.createTestTrain(srcKey='A.hex', trainDstKey=trainDataKey, testDstKey=testDataKey, trainPercent=90)
            aHack = {'destination_key': trainDataKey}
            parseKey = trainDataKey

            # GLM **********************************************8
            start = time.time()
            glm = h2o_cmd.runGLM(parseResult=aHack, timeoutSecs=timeoutSecs, pollTimeoutSecs=180, **kwargs)
            print "glm end on ", parseResult['destination_key'], 'took', time.time() - start, 'seconds'
            h2o_glm.simpleCheckGLM(self, glm, None, **kwargs)
            modelKey = glm['glm_model']['_key']

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
            self.assertLess(pctWrong, 8,"Should see less than 7% error (class = 4)")

            print "\nTest\n==========\n"
            print h2o_gbm.pp_cm(cm)

            print "Trial #", trial, "completed", "using %6.2f" % (rowsToUse*100.0/numRows), "pct. of all rows"

if __name__ == '__main__':
    h2o.unit_main()
