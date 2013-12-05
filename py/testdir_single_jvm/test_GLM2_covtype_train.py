import unittest, random, sys, time
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_hosts, h2o_import as h2i, h2o_exec, h2o_glm

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

    def test_GLM2_covtype_train(self):
        h2o.beta_features = True
        importFolderPath = "standard"
        csvFilename = 'covtype.shuffled.data'
        csvPathname = importFolderPath + "/" + csvFilename
        hex_key = csvFilename + ".hex"

        print "\nUsing header=0 on the normal covtype.data"
        parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathname, schema='put', hex_key=hex_key,
            header=0, timeoutSecs=180)

        inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
        print "\n" + csvPathname, \
            "    numRows:", "{:,}".format(inspect['numRows']), \
            "    numCols:", "{:,}".format(inspect['numCols'])

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
        
        h2o_cmd.createTestTrain(srcKey=hex_key, trainDstKey=trainDataKey, testDstKey=testDataKey, trainPercent=90)
        # will have to live with random extract. will create variance

        kwargs = {
            'response': 'C54', 
            'max_iter': 20, 
            'n_folds': 0, 
            'alpha': 0.1, 
            'lambda': 1e-5, 
            'family': 'binomial',
            'case_mode': '=', 
            'case_val': 4,
        }
        timeoutSecs = 60

        for trial in range(10):
            # always slice from the beginning
            rowsToUse = rowsForPct[trial%10] 

            h2o_cmd.createTestTrain(srcKey=hex_key, trainDstKey=trainDataKey, testDstKey=testDataKey, trainPercent=90)
            parseResult['destination_key'] = trainDataKey

            start = time.time()
            glm = h2o_cmd.runGLM(parseResult=parseResult, timeoutSecs=timeoutSecs, pollTimeoutSecs=180, **kwargs)
            print "glm end on ", parseResult['destination_key'], 'took', time.time() - start, 'seconds'
            h2o_glm.simpleCheckGLM(self, glm, None, **kwargs)
            modelKey = glm['glm_model']['_selfKey']

            start = time.time()
            glmScore = h2o_cmd.runGLMScore(key=testDataKey, model_key=modelKey, thresholds="0.5", timeoutSecs=timeoutSecs)
            print "glmScore end on ", testDataKey, 'took', time.time() - start, 'seconds'
            classErr = glmScore['validation']['classErr']
            auc = glmScore['validation']['auc']
            err = glmScore['validation']['err']
            print "classErr:", classErr
            print "err:", err
            print "auc:", auc

            print "Trial #", trial, "completed", "using %6.2f" % (rowsToUse*100.0/numRows), "pct. of all rows"

if __name__ == '__main__':
    h2o.unit_main()
