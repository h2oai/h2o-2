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

    def test_GLM2_tmp(self):
        importFolderPath = "/tmp"
        csvFilename = 's.csv'
        bcFilename = 'bc.csv'

        csvPathname = importFolderPath + "/" + csvFilename
        bcPathname = importFolderPath + "/" + bcFilename

        hex_key = csvFilename + ".hex"
        bc_key = bcFilename + ".hex"

        # Parse
        parseResult = h2i.import_parse(path=csvPathname, schema='put', hex_key=hex_key, timeoutSecs=180)
        inspect = h2o_cmd.runInspect(key=hex_key)
        print "\n" + csvPathname, \
            "    numRows:", "{:,}".format(inspect['numRows']), \
            "    numCols:", "{:,}".format(inspect['numCols'])

        bcResult = h2i.import_parse(path=bcPathname, schema='put', hex_key=bc_key, timeoutSecs=180)
        inspect = h2o_cmd.runInspect(key=bc_key)
        print "\n" + bcPathname, \
            "    numRows:", "{:,}".format(inspect['numRows']), \
            "    numCols:", "{:,}".format(inspect['numCols'])

        # Split Test/Train************************************************
        # how many rows for each pct?
        numRows = inspect['numRows']
        trainDataKey = hex_key
        testDataKey = hex_key
        
        # GLM, predict, CM*******************************************************8
        kwargs = {
            'response': "response",
            'non_negative': 0,
            'standardize': 1,
            'strong_rules': 1,
            'alpha': 0,
            'max_iter': 100,
            'lambda_min_ratio': -1,
            'higher_accuracy': 1,
            'beta_constraints': bc_key,
            'link': "family_default",
            'use_all_factor_levels': 0,
            'variable_importances': 0,
            'lambda': 0,
            'prior': 0.00301875221383974,
            'nlambdas': -1,
            'source': hex_key,
            'lambda_search': 0,
            'disable_line_search': 0,
            'n_folds': 0,
            'family': "binomial",
            'beta_epsilon': 1e-04,
            'intercept': 1,
            'max_predictors': -1,
            # "used_cols"':  "4,5,18,37,38,53,66,73,90,93,95,96,112,117,135,158,165,166,168,177,180",
            # 'ignored_cols': "1,2,3,4,5,6,7,8,9,11,12,14,15,16,17,18,19,20,21,22,23,24,25,26,27,29,31,32,34,35,36,37,38,40,41,42,43,44,45,46,47,48,49,51,52,53,54,55,56,57,58,59,60,61,62,63,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,91,92,93,94,95,96,97,98,100,101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,119,120,121,123,124,125,126,128,129,133,134,135,136,137,138,140,141,142,143,144,145,146,147,148,149,150,151,152,153,154,155,157,158,159,160,161,162,163,164,165,166,167,168,169,170,171,173,174,176,177,178,179",
        }

        timeoutSecs = 180

        for trial in range(10):
            parseKey = trainDataKey

            # GLM **********************************************8
            start = time.time()
            glm = h2o_cmd.runGLM(parseResult=parseResult, timeoutSecs=timeoutSecs, pollTimeoutSecs=180, **kwargs)
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
                vactual='response',
                predict=predictKey,
                vpredict='predict',
                )

            cm = predictCMResult['cm']

            # These will move into the h2o_gbm.py
            pctWrong = h2o_gbm.pp_cm_summary(cm);
            self.assertLess(pctWrong, 8,"Should see less than 7% error")

            print "\nTest\n==========\n"
            print h2o_gbm.pp_cm(cm)

            print "Trial #", trial, "completed"

if __name__ == '__main__':
    h2o.unit_main()
