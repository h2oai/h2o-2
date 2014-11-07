import unittest
import random, sys, time, re
sys.path.extend(['.','..','../..','py'])
import h2o_browse as h2b, h2o_gbm

import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i, h2o_glm, h2o_util, h2o_rf, h2o_jobs as h2j
class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.init(1, java_heap_GB=4)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_GBM_covtype_train_test(self):

        bucket = 'home-0xdiag-datasets'
        importFolderPath = 'standard'
        trainFilename = 'covtype.shuffled.90pct.data'
        trainKey = 'covtype.train.hex'
        modelKey = 'GBMModelKey'
        timeoutSecs = 1800

        parseTrainResult = h2i.import_parse(bucket=bucket, path=importFolderPath + "/" + trainFilename, schema='local',
            hex_key=trainKey, timeoutSecs=timeoutSecs)

        kwargs = {
            'max_depth': 7,
            # 'max_depth': 10,
            'ntrees': 2,
            'response': 'C55'
        }

        h2o_cmd.runGBM(parseResult=parseTrainResult, timeoutSecs=timeoutSecs, destination_key=modelKey, **kwargs)


if __name__ == '__main__':
    h2o.unit_main()
