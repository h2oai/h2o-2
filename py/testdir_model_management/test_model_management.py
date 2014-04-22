import unittest, time, sys, os
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts, h2o_import as h2i
import h2o_glm, h2o_gbm, h2o_rf # TODO: DeepLearning

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global localhost

        if h2o.clone_cloud_json is not None:
            print "NOTE: Connecting to existing cloud, and leaving the cloud running afterwards: " + os.path.abspath(h2o.clone_cloud_json)

        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(1)
        else:
            h2o_hosts.build_cloud_with_hosts(1)
        
        # USE FVec!
        h2o.beta_features = True
        Basic.import_frames()

    @classmethod
    def tearDownClass(cls):
        if h2o.clone_cloud_json is None:
            h2o.tear_down_cloud()

    def assertKeysExist(self, d, path, keys):
        path_elems = path.split("/")

        for path_elem in path_elems:
            if "" != path_elem:
                assert path_elem in d, "Failed to find key: " + path + " in dict: " + repr(d)
                d = d[path]
        
        for key in keys:
            assert key in d, "Failed to find key: " + key + " in dict: " + repr(d)

    def assertKeysDontExist(self, d, path, keys):
        path_elems = path.split("/")

        for path_elem in path_elems:
            if "" != path_elem:
                assert path_elem in d, "Unexpectedly failed to find key: " + path + " in dict: " + repr(d)
                d = d[path]
        
        for key in keys:
            assert key not in d, "Unexpectedly found key: " + key + " in dict: " + repr(d)


    prostate_hex = None
    airlines_train_hex = None
    airlines_test_hex = None

    @classmethod
    def import_frame(cls, target_key, bucket, csvFilename, csvPathname, expected_rows, expected_cols):
        path = csvPathname + '/' + csvFilename
        parseResult = h2i.import_parse(bucket=bucket, path=path, hex_key=target_key, schema='put') # upload the file
        destination_key = parseResult['destination_key']  # we block until it's actually ready

        inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
        h2o_cmd.infoFromInspect(inspect, csvPathname)
        actual_rows = inspect['numRows']
        actual_cols = inspect['numCols']

        print 'loaded frame "' + target_key +'" from path: ' + path
        print 'rows: ', actual_rows
        print 'cols: ', actual_cols

        # Don't have access to the testCase assert methods here because they aren't class methods. :-(
        assert expected_rows == actual_rows, "Expected " + str(expected_rows) + " but got " + str(actual_rows) + " for path: " + path
        assert expected_cols == actual_cols, "Expected " + str(expected_cols) + " but got " + str(actual_cols) + " for path: " + path

        # TODO: other info we could check
        # (missingValuesDict, constantValuesDict, enumSizeDict, colTypeDict, colNameDict) = \
        #     h2o_cmd.columnInfoFromInspect(parseResult['destination_key'], exceptionOnMissingValues=True)
        # 
        # summaryResult = h2o_cmd.runSummary(key=parseResult['destination_key'])
        # h2o_cmd.infoFromSummary(summaryResult) # , noPrint=True
        return destination_key


    @classmethod
    def import_frames(cls):
        Basic.prostate_hex = Basic.import_frame('prostate.hex', 'smalldata', 'prostate.csv', 'logreg', 380, 9)
        Basic.airlines_train_hex = Basic.import_frame('airlines_train.hex', 'smalldata', 'AirlinesTrain.csv.zip', 'airlines', 24421, 12)
        Basic.airlines_test_hex = Basic.import_frame('airlines_test.hex', 'smalldata', 'AirlinesTest.csv.zip', 'airlines', 2691, 12)
        

    # this is my test!
    def test_binary_classifiers(self):
        self.assertIsNotNone(Basic.prostate_hex)
        self.assertIsNotNone(Basic.airlines_train_hex)
        self.assertIsNotNone(Basic.airlines_test_hex)

        node = h2o.nodes[0]
        timeoutSecs = 200
        retryDelaySecs = 2

        print "##############################################################"
        print "Generating AirlinesTrain GLM2 binary classification model. . ."
        # h2o.glm.FV(y = "IsDepDelayed", x = c("Origin", "Dest", "fDayofMonth", "fYear", "UniqueCarrier", "fDayOfWeek", "fMonth", "DepTime", "ArrTime", "Distance"), data = airlines_train.hex, family = "binomial", alpha=0.05, lambda=1.0e-2, standardize=FALSE, nfolds=0)
        glm_AirlinesTrain_1_params = {
            'destination_key': 'glm_AirlinesTrain_binary_1',
            'response': 'IsDepDelayed', 
            'ignored_cols': 'IsDepDelayed_REC', 
            'family': 'binomial', 
            'alpha': 0.5, 
            'standardize': 0, 
            'lambda': 1.0e-2, 
            'n_folds': 0
        }
        glm_AirlinesTrain_1 = node.GLM(Basic.airlines_train_hex, timeoutSecs, retryDelaySecs, **glm_AirlinesTrain_1_params)
        h2o_glm.simpleCheckGLM(self, glm_AirlinesTrain_1, None, **glm_AirlinesTrain_1_params)


        print "####################################################################"
        print "Generating AirlinesTrain simple GBM binary classification model. . ."
        # h2o.gbm(y = "IsDepDelayed", x = c("Origin", "Dest", "fDayofMonth", "fYear", "UniqueCarrier", "fDayOfWeek", "fMonth", "DepTime", "ArrTime", "Distance"), data = airlines_train.hex, n.trees=3, interaction.depth=1, distribution="multinomial", n.minobsinnode=2, shrinkage=.1)
        gbm_AirlinesTrain_1_params = {
            'destination_key': 'gbm_AirlinesTrain_binary_1',
            'response': 'IsDepDelayed', 
            'ignored_cols_by_name': 'IsDepDelayed_REC', 
            'ntrees': 3,
            'max_depth': 1,
            'classification': 1
            # TODO: what about minobsinnode and shrinkage?!
        }
        gbm_AirlinesTrain_1 = node.gbm(Basic.airlines_train_hex, timeoutSecs, retryDelaySecs, **gbm_AirlinesTrain_1_params)


        print "#####################################################################"
        print "Generating AirlinesTrain complex GBM binary classification model. . ."
        # h2o.gbm(y = "IsDepDelayed", x = c("Origin", "Dest", "fDayofMonth", "fYear", "UniqueCarrier", "fDayOfWeek", "fMonth", "DepTime", "ArrTime", "Distance"), data = airlines_train.hex, n.trees=50, interaction.depth=5, distribution="multinomial", n.minobsinnode=2, shrinkage=.1)
        gbm_AirlinesTrain_2_params = {
            'destination_key': 'gbm_AirlinesTrain_binary_2',
            'response': 'IsDepDelayed', 
            'ignored_cols_by_name': 'IsDepDelayed_REC', 
            'ntrees': 50,
            'max_depth': 5,
            'classification': 1
            # TODO: what about minobsinnode and shrinkage?!
        }
        gbm_AirlinesTrain_2 = node.gbm(Basic.airlines_train_hex, timeoutSecs, retryDelaySecs, **gbm_AirlinesTrain_2_params)


        print "####################################################################"
        print "Generating AirlinesTrain simple DRF binary classification model. . ."
        # h2o.randomForest.FV(y = "IsDepDelayed", x = c("Origin", "Dest", "fDayofMonth", "fYear", "UniqueCarrier", "fDayOfWeek", "fMonth", "DepTime", "ArrTime", "Distance"), data = airlines_train.hex, ntree=5, depth=2)
        rf_AirlinesTrain_1_params = {
            'destination_key': 'rf_AirlinesTrain_binary_1',
            'response': 'IsDepDelayed', 
            'ignored_cols_by_name': 'IsDepDelayed_REC', 
            'ntrees': 5,
            'max_depth': 2,
            'classification': 1
        }
        rf_AirlinesTrain_1 = node.random_forest(Basic.airlines_train_hex, timeoutSecs, retryDelaySecs, **rf_AirlinesTrain_1_params)


        print "#####################################################################"
        print "Generating AirlinesTrain complex DRF binary classification model. . ."
        # h2o.randomForest.FV(y = "IsDepDelayed", x = c("Origin", "Dest", "fDayofMonth", "fYear", "UniqueCarrier", "fDayOfWeek", "fMonth", "DepTime", "ArrTime", "Distance"), data = airlines_train.hex, ntree=50, depth=10)
        rf_AirlinesTrain_2_params = {
            'destination_key': 'rf_AirlinesTrain_binary_2',
            'response': 'IsDepDelayed', 
            'ignored_cols_by_name': 'IsDepDelayed_REC', 
            'ntrees': 50,
            'max_depth': 10,
            'classification': 1
        }
        rf_AirlinesTrain_2 = node.random_forest(Basic.airlines_train_hex, timeoutSecs, retryDelaySecs, **rf_AirlinesTrain_2_params)


        print "######################################################################"
        print "Generating AirlinesTrain DeepLearning binary classification model. . ."
        # h2o.deeplearning(y = "IsDepDelayed", x = c("Origin", "Dest", "fDayofMonth", "fYear", "UniqueCarrier", "fDayOfWeek", "fMonth", "DepTime", "ArrTime", "Distance"), data = airlines_train.hex, classification=TRUE, hidden=c(10, 10))
        dl_AirlinesTrain_1_params = {
            'destination_key': 'dl_AirlinesTrain_binary_1',
            'response': 'IsDepDelayed', 
            'ignored_cols': 'IsDepDelayed_REC', 
            'hidden': [10, 10],
            'classification': 1
        }
        dl_AirlinesTrain_1 = node.deep_learning(Basic.airlines_train_hex, timeoutSecs, retryDelaySecs, **dl_AirlinesTrain_1_params)


        print "##############################################################################################"
        print "Generating AirlinesTrain GLM2 binary classification model with different response column. . ."
        # h2o.glm.FV(y = "IsDepDelayed_REC", x = c("Origin", "Dest", "fDayofMonth", "fYear", "UniqueCarrier", "fDayOfWeek", "fMonth", "DepTime", "ArrTime", "Distance"), data = airlines_train.hex, family = "binomial", alpha=0.05, lambda=1.0e-2, standardize=FALSE, nfolds=0)
        glm_AirlinesTrain_A_params = {
            'destination_key': 'glm_AirlinesTrain_binary_A',
            'response': 'IsDepDelayed_REC', 
            'ignored_cols': 'IsDepDelayed', 
            'family': 'binomial', 
            'alpha': 0.5, 
            'standardize': 0, 
            'lambda': 1.0e-2, 
            'n_folds': 0
        }
        glm_AirlinesTrain_A = node.GLM(Basic.airlines_train_hex, timeoutSecs, retryDelaySecs, **glm_AirlinesTrain_A_params)
        h2o_glm.simpleCheckGLM(self, glm_AirlinesTrain_A, None, **glm_AirlinesTrain_A_params)


        print "#########################################################"
        print "Generating Prostate GLM2 binary classification model. . ."
        # h2o.glm.FV(y = "CAPSULE", x = c("AGE","RACE","PSA","DCAPS"), data = prostate.hex, family = "binomial", nfolds = 0, alpha = 0.5)
        glm_Prostate_1_params = {
            'destination_key': 'glm_Prostate_binary_1',
            'response': 'CAPSULE', 
            'ignored_cols': None, 
            'family': 'binomial', 
            'alpha': 0.5, 
            'n_folds': 0
        }
        glm_Prostate_1 = node.GLM(Basic.prostate_hex, timeoutSecs, retryDelaySecs, **glm_Prostate_1_params)
        h2o_glm.simpleCheckGLM(self, glm_Prostate_1, None, **glm_Prostate_1_params)


        print "###############################################################"
        print "Generating Prostate simple DRF binary classification model. . ."
        # h2o.randomForest.FV(y = "CAPSULE", x = c("AGE","RACE","DCAPS"), data = prostate.hex, ntree=10, depth=5)
        rf_Prostate_1_params = {
            'destination_key': 'rf_Prostate_binary_1',
            'response': 'CAPSULE', 
            'ignored_cols_by_name': None, 
            'ntrees': 10,
            'max_depth': 5,
            'classification': 1
        }
        rf_Prostate_1 = node.random_forest(Basic.prostate_hex, timeoutSecs, retryDelaySecs, **rf_Prostate_1_params)


        print "##############################################"
        print "Generating Prostate GLM2 regression model. . ."
        # h2o.glm.FV(y = "AGE", x = c("CAPSULE","RACE","PSA","DCAPS"), data = prostate.hex, family = "gaussian", nfolds = 0, alpha = 0.5)
        glm_Prostate_regression_1_params = {
            'destination_key': 'glm_Prostate_regression_1',
            'response': 'AGE', 
            'ignored_cols': None, 
            'family': 'gaussian', 
            'alpha': 0.5, 
            'n_folds': 0
        }
        glm_Prostate_regression_1 = node.GLM(Basic.prostate_hex, timeoutSecs, retryDelaySecs, **glm_Prostate_regression_1_params)
        h2o_glm.simpleCheckGLM(self, glm_Prostate_regression_1, None, **glm_Prostate_regression_1_params)


        print "##############################################"
        print "Testing /2/Frames with various options. . ."
        print "##############################################"
        print ""


        print "##############################################"
        print "Testing /2/Frames list. . ."
        frames = node.frames()
        self.assertKeysExist(frames, 'frames', ['airlines_train.hex', 'airlines_test.hex', 'prostate.hex'])
        self.assertKeysDontExist(frames, 'frames', ['glm_AirlinesTrain_binary_1', 'gbm_AirlinesTrain_binary_1', 'gbm_AirlinesTrain_binary_2', 'rf_AirlinesTrain_binary_1', 'rf_AirlinesTrain_binary_2', 'dl_AirlinesTrain_binary_1', 'glm_AirlinesTrain_binary_A', 'glm_Prostate_binary_1', 'rf_Prostate_binary_1', 'glm_Prostate_regression_1'])
        self.assertKeysDontExist(frames, '', ['models'])


        print "##############################################"
        print "Testing /2/Frames?key=airlines_test.hex. . ."
        frames = node.frames(key='airlines_test.hex')
        self.assertKeysExist(frames, 'frames', ['airlines_test.hex'])
        self.assertKeysDontExist(frames, 'frames', ['glm_AirlinesTrain_binary_1', 'gbm_AirlinesTrain_binary_1', 'gbm_AirlinesTrain_binary_2', 'rf_AirlinesTrain_binary_1', 'rf_AirlinesTrain_binary_2', 'dl_AirlinesTrain_binary_1', 'glm_AirlinesTrain_binary_A', 'glm_Prostate_binary_1', 'rf_Prostate_binary_1', 'glm_Prostate_regression_1', 'airlines_train.hex', 'prostate.hex'])
        self.assertKeysDontExist(frames, '', ['models'])


        print "##############################################"
        print "Testing /2/Frames?key=airlines_test.hex&find_compatible_models=true. . ."
        frames = node.frames(key='airlines_test.hex', find_compatible_models=1)
        self.assertKeysExist(frames, 'frames', ['airlines_test.hex'])
        self.assertKeysDontExist(frames, 'frames', ['glm_AirlinesTrain_binary_1', 'gbm_AirlinesTrain_binary_1', 'gbm_AirlinesTrain_binary_2', 'rf_AirlinesTrain_binary_1', 'rf_AirlinesTrain_binary_2', 'dl_AirlinesTrain_binary_1', 'glm_AirlinesTrain_binary_A', 'glm_Prostate_binary_1', 'rf_Prostate_binary_1', 'glm_Prostate_regression_1', 'airlines_train.hex', 'prostate.hex'])

        self.assertKeysExist(frames, '', ['models'])
        self.assertKeysExist(frames, 'models', ['glm_AirlinesTrain_binary_1', 'gbm_AirlinesTrain_binary_1', 'gbm_AirlinesTrain_binary_2', 'rf_AirlinesTrain_binary_1', 'rf_AirlinesTrain_binary_2', 'dl_AirlinesTrain_binary_1', 'glm_AirlinesTrain_binary_A'])
        self.assertKeysDontExist(frames, 'models', ['glm_Prostate_binary_1', 'rf_Prostate_binary_1', 'glm_Prostate_regression_1', 'airlines_train.hex', 'airlines_train.hex', 'airlines_test.hex', 'prostate.hex'])


        print "##############################################"
        print "Testing /2/Frames with various options. . ."
        print "##############################################"
        print ""


        print "##############################################"
        print "Testing /2/Models list. . ."
        models = node.models()
        self.assertKeysExist(models, 'models', ['glm_AirlinesTrain_binary_1', 'gbm_AirlinesTrain_binary_1', 'gbm_AirlinesTrain_binary_2', 'rf_AirlinesTrain_binary_1', 'rf_AirlinesTrain_binary_2', 'dl_AirlinesTrain_binary_1', 'glm_AirlinesTrain_binary_A', 'glm_Prostate_binary_1', 'rf_Prostate_binary_1', 'glm_Prostate_regression_1'])
        self.assertKeysDontExist(models, 'models', ['airlines_train.hex', 'airlines_test.hex', 'prostate.hex'])
        self.assertKeysDontExist(models, '', ['frames'])


        print "##############################################"
        print "Testing /2/Models?key=rf_Prostate_binary_1. . ."
        models = node.models(key='rf_Prostate_binary_1')
        self.assertKeysExist(models, 'models', ['rf_Prostate_binary_1'])
        self.assertKeysDontExist(models, 'models', ['airlines_train.hex', 'airlines_test.hex', 'prostate.hex', 'glm_AirlinesTrain_binary_1', 'gbm_AirlinesTrain_binary_1', 'gbm_AirlinesTrain_binary_2', 'rf_AirlinesTrain_binary_1', 'rf_AirlinesTrain_binary_2', 'dl_AirlinesTrain_binary_1', 'glm_AirlinesTrain_binary_A', 'glm_Prostate_binary_1', 'glm_Prostate_regression_1'])
        self.assertKeysDontExist(models, '', ['frames'])


        print "##############################################"
        print "Testing /2/Models?key=rf_Prostate_binary_1&find_compatible_frames=true. . ."
        models = node.models(key='rf_Prostate_binary_1', find_compatible_frames=1)
        self.assertKeysExist(models, 'models', ['rf_Prostate_binary_1'])
        self.assertKeysDontExist(models, 'models', ['airlines_train.hex', 'airlines_test.hex', 'prostate.hex', 'glm_AirlinesTrain_binary_1', 'gbm_AirlinesTrain_binary_1', 'gbm_AirlinesTrain_binary_2', 'rf_AirlinesTrain_binary_1', 'rf_AirlinesTrain_binary_2', 'dl_AirlinesTrain_binary_1', 'glm_AirlinesTrain_binary_A', 'glm_Prostate_binary_1', 'glm_Prostate_regression_1'])

        self.assertKeysExist(models, '', ['frames'])
        self.assertKeysExist(models, 'frames', ['prostate.hex'])
        self.assertKeysDontExist(models, 'frames', ['airlines_train.hex', 'airlines_test.hex'])


        print "##############################################"
        print "Testing /2/Frames with scoring. . ."
        print "##############################################"
        print ""


        print "##############################################"
        print "Scoring compatible models for /2/Frames?key=airlines_test.hex&find_compatible_models=true. . ."
        frames = node.frames(key='airlines_test.hex', find_compatible_models=1)
        compatible_models = frames['frames']['airlines_test.hex']['compatible_models']

        for model_key in compatible_models:
            print "Scoring: /2/Frames?key=airlines_test.hex&score_model=" + model_key
            scoring_result = node.frames(key='airlines_test.hex', score_model=model_key)
            self.assertKeysExist(scoring_result, '', ['metrics'])
            self.assertKeysExist(scoring_result, 'metrics', ['model', 'frame', 'duration_in_ms', 'error', 'cm', 'auc']) # TODO: HitRatio
            self.assertEqual(scoring_result['metrics']['model'], model_key, "Expected model key: " + model_key + " but got: " + scoring_result['metrics']['model'])
            self.assertEqual(scoring_result['metrics']['frame'], 'airlines_test.hex', "Expected frame key: " + 'airlines_test.hex' + " but got: " + scoring_result['metrics']['frame'])
            # TODO: look inside the auc and cm elements


        print "##############################################"
        print "Scoring compatible models for /2/Frames?key=prostate.hex&find_compatible_models=true. . ."
        frames = node.frames(key='prostate.hex', find_compatible_models=1)
        compatible_models = frames['frames']['prostate.hex']['compatible_models']

        for model_key in compatible_models:
            print "Scoring: /2/Frames?key=prostate.hex&score_model=" + model_key
            scoring_result = node.frames(key='prostate.hex', score_model=model_key)
            self.assertKeysExist(scoring_result, '', ['metrics'])
            self.assertKeysExist(scoring_result, 'metrics', ['model', 'frame', 'duration_in_ms', 'error', 'cm', 'auc']) # TODO: HitRatio
            self.assertEqual(scoring_result['metrics']['model'], model_key, "Expected model key: " + model_key + " but got: " + scoring_result['metrics']['model'])
            self.assertEqual(scoring_result['metrics']['frame'], 'prostate.hex', "Expected frame key: " + 'prostate.hex' + " but got: " + scoring_result['metrics']['frame'])
            # TODO: look inside the auc and cm elements


if __name__ == '__main__':
    h2o.unit_main()
