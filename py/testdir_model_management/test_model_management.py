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
        timeoutSecs = 20
        retryDelaySecs = 2

        ################################################
        print "Generating AirlinesTrain GLM2 model. . ."
        # h2o.glm.FV(y = "IsDepDelayed", x = c("Origin", "Dest", "fDayofMonth", "fYear", "UniqueCarrier", "fDayOfWeek", "fMonth", "DepTime", "ArrTime", "Distance"), data = airlines_train.hex, family = "binomial", alpha=0.05, lambda=1.0e-2, standardize=FALSE, nfolds=0)
        glm_AirlinesTrain_1_params = {
            'destination_key': 'glm_AirlinesTrain_1',
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


        ######################################################
        print "Generating AirlinesTrain simple GBM model. . ."
        # h2o.gbm(y = "IsDepDelayed", x = c("Origin", "Dest", "fDayofMonth", "fYear", "UniqueCarrier", "fDayOfWeek", "fMonth", "DepTime", "ArrTime", "Distance"), data = airlines_train.hex, n.trees=3, interaction.depth=1, distribution="multinomial", n.minobsinnode=2, shrinkage=.1)
        gbm_AirlinesTrain_1_params = {
            'destination_key': 'gbm_AirlinesTrain_1',
            'response': 'IsDepDelayed', 
            'ignored_cols_by_name': 'IsDepDelayed_REC', 
            'ntrees': 3,
            'max_depth': 1,
            'classification': 1
            # TODO: what about minobsinnode and shrinkage?!
        }
        gbm_AirlinesTrain_1 = node.gbm(Basic.airlines_train_hex, timeoutSecs, retryDelaySecs, **gbm_AirlinesTrain_1_params)


        ######################################################
        print "Generating AirlinesTrain complex GBM model. . ."
        # h2o.gbm(y = "IsDepDelayed", x = c("Origin", "Dest", "fDayofMonth", "fYear", "UniqueCarrier", "fDayOfWeek", "fMonth", "DepTime", "ArrTime", "Distance"), data = airlines_train.hex, n.trees=50, interaction.depth=5, distribution="multinomial", n.minobsinnode=2, shrinkage=.1)
        gbm_AirlinesTrain_2_params = {
            'destination_key': 'gbm_AirlinesTrain_2',
            'response': 'IsDepDelayed', 
            'ignored_cols_by_name': 'IsDepDelayed_REC', 
            'ntrees': 50,
            'max_depth': 5,
            'classification': 1
            # TODO: what about minobsinnode and shrinkage?!
        }
        gbm_AirlinesTrain_2 = node.gbm(Basic.airlines_train_hex, timeoutSecs, retryDelaySecs, **gbm_AirlinesTrain_2_params)


        ######################################################
        print "Generating AirlinesTrain simple DRF model. . ."
        # h2o.randomForest.FV(y = "IsDepDelayed", x = c("Origin", "Dest", "fDayofMonth", "fYear", "UniqueCarrier", "fDayOfWeek", "fMonth", "DepTime", "ArrTime", "Distance"), data = airlines_train.hex, ntree=5, depth=2)
        rf_AirlinesTrain_1_params = {
            'destination_key': 'rf_AirlinesTrain_1',
            'response': 'IsDepDelayed', 
            'ignored_cols_by_name': 'IsDepDelayed_REC', 
            'ntrees': 5,
            'max_depth': 2,
            'classification': 1
        }
        rf_AirlinesTrain_1 = node.random_forest(Basic.airlines_train_hex, timeoutSecs, retryDelaySecs, **rf_AirlinesTrain_1_params)


        #######################################################
        print "Generating AirlinesTrain complex DRF model. . ."
        # h2o.randomForest.FV(y = "IsDepDelayed", x = c("Origin", "Dest", "fDayofMonth", "fYear", "UniqueCarrier", "fDayOfWeek", "fMonth", "DepTime", "ArrTime", "Distance"), data = airlines_train.hex, ntree=50, depth=10)
        rf_AirlinesTrain_2_params = {
            'destination_key': 'rf_AirlinesTrain_2',
            'response': 'IsDepDelayed', 
            'ignored_cols_by_name': 'IsDepDelayed_REC', 
            'ntrees': 50,
            'max_depth': 10,
            'classification': 1
        }
        rf_AirlinesTrain_2 = node.random_forest(Basic.airlines_train_hex, timeoutSecs, retryDelaySecs, **rf_AirlinesTrain_2_params)


        # print("Generating AirlinesTrain DeepLearning model. . .")
        # h2o.deeplearning(y = "IsDepDelayed", x = c("Origin", "Dest", "fDayofMonth", "fYear", "UniqueCarrier", "fDayOfWeek", "fMonth", "DepTime", "ArrTime", "Distance"), data = airlines_train.hex, classification=TRUE, hidden=c(10, 10))



        #################################################################################
        # print("Generating AirlinesTrain GLM2 model with different response column. . .")
        # h2o.glm.FV(y = "IsDepDelayed_REC", x = c("Origin", "Dest", "fDayofMonth", "fYear", "UniqueCarrier", "fDayOfWeek", "fMonth", "DepTime", "ArrTime", "Distance"), data = airlines_train.hex, family = "binomial", alpha=0.05, lambda=1.0e-2, standardize=FALSE, nfolds=0)
        glm_AirlinesTrain_A_params = {
            'destination_key': 'glm_AirlinesTrain_A',
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




        # h2o_cmd.runRF(parseResult=parseResult, trees=50, timeoutSecs=10)

if __name__ == '__main__':
    h2o.unit_main()
