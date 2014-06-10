import unittest, time, sys, os
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts, h2o_import as h2i, h2o_exec
import h2o_glm, h2o_gbm, h2o_rf # TODO: DeepLearning

class ModelManagementTestCase(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global localhost

        if h2o.clone_cloud_json is not None:
            print "NOTE: Connecting to existing cloud, and leaving the cloud running afterwards: " + os.path.abspath(h2o.clone_cloud_json)

        localhost = h2o.decide_if_localhost()
        if (localhost):
            print "Calling h2o.build_cloud(1). . ."
            h2o.build_cloud(1)
        else:
            h2o_hosts.build_cloud_with_hosts(1)
            print "Calling h2o_hosts.build_cloud_with_hosts(1). . ."
        
        # USE FVec!
        h2o.beta_features = True

    @classmethod
    def tearDownClass(cls):
        if h2o.clone_cloud_json is None:
            h2o.tear_down_cloud()
        else:
            h2o.check_sandbox_for_errors(sandboxIgnoreErrors=False, python_test_name="test_model_management")


    already_set_up = False

    ''' Lazy setup of the common frames and models used by the test cases. '''
    def setUp(self):
        if ModelManagementTestCase.already_set_up:
            return
        self.create_models(self.import_frames())
        ModelManagementTestCase.already_set_up = True


    def import_frame(self, target_key, bucket, csvFilename, csvPathname, expected_rows, expected_cols):
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


    # TODO: generalize by passing in the exec2 expression
    def create_new_boolean(self, frame, old_col_name, new_col_name):
        node = h2o.nodes[0]

        # NOTE: 1-based column indexing!

        resultExec, ncols = h2o_exec.exec_expr(execExpr='ncol(' + frame + ')')
        # print 'before doing anything, ncols: ', int(ncols)

        resultExec, dontcare = h2o_exec.exec_expr(execExpr="{0}[, ncol({0}) + 1] = ({0}${1} == 1)".format(frame, old_col_name))
        resultExec, ncols = h2o_exec.exec_expr(execExpr="ncol({0})".format(frame))

        ncols = int(ncols)
        # print 'after allegedly creating new column ncols: ', ncols

        node.set_column_names(source=frame, cols='C' + str(ncols), comma_separated_list=new_col_name)


    def import_frames(self):
        prostate_hex = self.import_frame('prostate.hex', 'smalldata', 'prostate.csv', 'logreg', 380, 9)
        airlines_train_hex = self.import_frame('airlines_train.hex', 'smalldata', 'AirlinesTrain.csv.zip', 'airlines', 24421, 12)
        airlines_test_hex = self.import_frame('airlines_test.hex', 'smalldata', 'AirlinesTest.csv.zip', 'airlines', 2691, 12)

        self.create_new_boolean('airlines_train.hex', 'IsDepDelayed_REC', 'IsDepDelayed_REC_recoded')
        self.create_new_boolean('airlines_test.hex', 'IsDepDelayed_REC', 'IsDepDelayed_REC_recoded')

        return (prostate_hex, airlines_train_hex, airlines_test_hex)
        

    def create_models(self, frame_keys):

        prostate_hex, airlines_train_hex, airlines_test_hex = frame_keys

        self.assertIsNotNone(prostate_hex)
        self.assertIsNotNone(airlines_train_hex)
        self.assertIsNotNone(airlines_test_hex)

        node = h2o.nodes[0]

        print "##############################################################"
        print "Generating AirlinesTrain GLM2 binary classification model. . ."
        # h2o.glm.FV(y = "IsDepDelayed", x = c("Origin", "Dest", "fDayofMonth", "fYear", "UniqueCarrier", "fDayOfWeek", "fMonth", "DepTime", "ArrTime", "Distance"), data = airlines_train.hex, family = "binomial", alpha=0.05, lambda=1.0e-2, standardize=FALSE, nfolds=0)
        glm_AirlinesTrain_1_params = {
            'destination_key': 'glm_AirlinesTrain_binary_1',
            'response': 'IsDepDelayed', 
            'ignored_cols': 'IsDepDelayed_REC, IsDepDelayed_REC_recoded', 
            'family': 'binomial', 
            'alpha': 0.5, 
            'standardize': 0, 
            'lambda': 1.0e-2, 
            'n_folds': 0
        }
        glm_AirlinesTrain_1 = node.GLM(airlines_train_hex, **glm_AirlinesTrain_1_params)
        h2o_glm.simpleCheckGLM(self, glm_AirlinesTrain_1, None, **glm_AirlinesTrain_1_params)


        print "####################################################################"
        print "Generating AirlinesTrain simple GBM binary classification model. . ."
        # h2o.gbm(y = "IsDepDelayed", x = c("Origin", "Dest", "fDayofMonth", "fYear", "UniqueCarrier", "fDayOfWeek", "fMonth", "DepTime", "ArrTime", "Distance"), data = airlines_train.hex, n.trees=3, interaction.depth=1, distribution="multinomial", n.minobsinnode=2, shrinkage=.1)
        gbm_AirlinesTrain_1_params = {
            'destination_key': 'gbm_AirlinesTrain_binary_1',
            'response': 'IsDepDelayed', 
            'ignored_cols_by_name': 'IsDepDelayed_REC, IsDepDelayed_REC_recoded', 
            'ntrees': 3,
            'max_depth': 1,
            'classification': 1
            # TODO: what about minobsinnode and shrinkage?!
        }
        gbm_AirlinesTrain_1 = node.gbm(airlines_train_hex, **gbm_AirlinesTrain_1_params)


        print "#####################################################################"
        print "Generating AirlinesTrain complex GBM binary classification model. . ."
        # h2o.gbm(y = "IsDepDelayed", x = c("Origin", "Dest", "fDayofMonth", "fYear", "UniqueCarrier", "fDayOfWeek", "fMonth", "DepTime", "ArrTime", "Distance"), data = airlines_train.hex, n.trees=50, interaction.depth=5, distribution="multinomial", n.minobsinnode=2, shrinkage=.1)
        gbm_AirlinesTrain_2_params = {
            'destination_key': 'gbm_AirlinesTrain_binary_2',
            'response': 'IsDepDelayed', 
            'ignored_cols_by_name': 'IsDepDelayed_REC, IsDepDelayed_REC_recoded', 
            'ntrees': 50,
            'max_depth': 5,
            'classification': 1
            # TODO: what about minobsinnode and shrinkage?!
        }
        gbm_AirlinesTrain_2 = node.gbm(airlines_train_hex, **gbm_AirlinesTrain_2_params)


        print "####################################################################"
        print "Generating AirlinesTrain simple DRF binary classification model. . ."
        # h2o.randomForest.FV(y = "IsDepDelayed", x = c("Origin", "Dest", "fDayofMonth", "fYear", "UniqueCarrier", "fDayOfWeek", "fMonth", "DepTime", "ArrTime", "Distance"), data = airlines_train.hex, ntree=5, depth=2)
        rf_AirlinesTrain_1_params = {
            'destination_key': 'rf_AirlinesTrain_binary_1',
            'response': 'IsDepDelayed', 
            'ignored_cols_by_name': 'IsDepDelayed_REC, IsDepDelayed_REC_recoded', 
            'ntrees': 5,
            'max_depth': 2,
            'classification': 1
        }
        rf_AirlinesTrain_1 = node.random_forest(airlines_train_hex, **rf_AirlinesTrain_1_params)


        print "#####################################################################"
        print "Generating AirlinesTrain complex DRF binary classification model. . ."
        # h2o.randomForest.FV(y = "IsDepDelayed", x = c("Origin", "Dest", "fDayofMonth", "fYear", "UniqueCarrier", "fDayOfWeek", "fMonth", "DepTime", "ArrTime", "Distance"), data = airlines_train.hex, ntree=50, depth=10)
        rf_AirlinesTrain_2_params = {
            'destination_key': 'rf_AirlinesTrain_binary_2',
            'response': 'IsDepDelayed', 
            'ignored_cols_by_name': 'IsDepDelayed_REC, IsDepDelayed_REC_recoded', 
            'ntrees': 50,
            'max_depth': 10,
            'classification': 1
        }
        rf_AirlinesTrain_2 = node.random_forest(airlines_train_hex, **rf_AirlinesTrain_2_params)


        print "#####################################################################"
        print "Generating AirlinesTrain complex SpeeDRF binary classification model. . ."
        # what is the R binding?
        speedrf_AirlinesTrain_1_params = {
            'destination_key': 'speedrf_AirlinesTrain_binary_1',
            'response': 'IsDepDelayed', 
            'ignored_cols_by_name': 'IsDepDelayed_REC, IsDepDelayed_REC_recoded', 
            'ntrees': 50,
            'max_depth': 10,
            'classification': 1
        }
        speedrf_AirlinesTrain_1 = node.speedrf(airlines_train_hex, **speedrf_AirlinesTrain_1_params)


        print "######################################################################"
        print "Generating AirlinesTrain DeepLearning binary classification model. . ."
        # h2o.deeplearning(y = "IsDepDelayed", x = c("Origin", "Dest", "fDayofMonth", "fYear", "UniqueCarrier", "fDayOfWeek", "fMonth", "DepTime", "ArrTime", "Distance"), data = airlines_train.hex, classification=TRUE, hidden=c(10, 10))
        dl_AirlinesTrain_1_params = {
            'destination_key': 'dl_AirlinesTrain_binary_1',
            'response': 'IsDepDelayed', 
            'ignored_cols': 'IsDepDelayed_REC, IsDepDelayed_REC_recoded', 
            'hidden': [10, 10],
            'classification': 1
        }
        dl_AirlinesTrain_1 = node.deep_learning(airlines_train_hex, **dl_AirlinesTrain_1_params)


        print "##############################################################################################"
        print "Generating AirlinesTrain GLM2 binary classification model with different response column. . ."
        # h2o.glm.FV(y = "IsDepDelayed_REC", x = c("Origin", "Dest", "fDayofMonth", "fYear", "UniqueCarrier", "fDayOfWeek", "fMonth", "DepTime", "ArrTime", "Distance"), data = airlines_train.hex, family = "binomial", alpha=0.05, lambda=1.0e-2, standardize=FALSE, nfolds=0)
        glm_AirlinesTrain_A_params = {
            'destination_key': 'glm_AirlinesTrain_binary_A',
            'response': 'IsDepDelayed_REC_recoded', 
            'ignored_cols': 'IsDepDelayed, IsDepDelayed_REC', 
            'family': 'binomial', 
            'alpha': 0.5, 
            'standardize': 0, 
            'lambda': 1.0e-2, 
            'n_folds': 0
        }
        glm_AirlinesTrain_A = node.GLM(airlines_train_hex, **glm_AirlinesTrain_A_params)
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
        glm_Prostate_1 = node.GLM(prostate_hex, **glm_Prostate_1_params)
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
        rf_Prostate_1 = node.random_forest(prostate_hex, **rf_Prostate_1_params)


        print "#####################################################################"
        print "Generating Prostate complex SpeeDRF binary classification model. . ."
        speedrf_Prostate_1_params = {
            'destination_key': 'speedrf_Prostate_binary_1',
            'response': 'CAPSULE', 
            'ignored_cols_by_name': None, 
            'ntrees': 50,
            'max_depth': 10,
            'classification': 1
        }
        speedrf_Prostate_1 = node.speedrf(prostate_hex, **speedrf_Prostate_1_params)


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
        glm_Prostate_regression_1 = node.GLM(prostate_hex, **glm_Prostate_regression_1_params)
        h2o_glm.simpleCheckGLM(self, glm_Prostate_regression_1, None, **glm_Prostate_regression_1_params)




class ApiTestCase(ModelManagementTestCase):

    def followPath(self, d, path_elems):
        for path_elem in path_elems:
            if "" != path_elem:
                idx = -1
                if path_elem.endswith("]"):
                    idx = int(path_elem[path_elem.find("[") + 1:path_elem.find("]")])
                    path_elem = path_elem[:path_elem.find("[")]
                assert path_elem in d, "Failed to find key: " + path_elem + " in dict: " + repr(d)

                if -1 == idx:
                    d = d[path_elem]
                else:
                    d = d[path_elem][idx]
        
        return d

    def assertKeysExist(self, d, path, keys):
        path_elems = path.split("/")

        d = self.followPath(d, path_elems)
        for key in keys:
            assert key in d, "Failed to find key: " + key + " in dict: " + repr(d)

    def assertKeysDontExist(self, d, path, keys):
        path_elems = path.split("/")

        d = self.followPath(d, path_elems)
        for key in keys:
            assert key not in d, "Unexpectedly found key: " + key + " in dict: " + repr(d)


    def test_endpoints(self):
        node = h2o.nodes[0]

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
        self.assertKeysExist(frames, 'frames/airlines_test.hex', ['creation_epoch_time_millis', 'id', 'key', 'column_names', 'compatible_models'])
        self.assertEqual(frames['frames']['airlines_test.hex']['id'], "88e9f821080b1221", msg="The airlines_test.hex frame hash should be deterministic.")
        self.assertEqual(frames['frames']['airlines_test.hex']['key'], "airlines_test.hex", msg="The airlines_test.hex key should be airlines_test.hex.")


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
        self.assertKeysExist(models, 'models/glm_AirlinesTrain_binary_1', ['id', 'key', 'creation_epoch_time_millis', 'model_category', 'state', 'input_column_names', 'response_column_name', 'critical_parameters', 'secondary_parameters', 'expert_parameters', 'compatible_frames'])
        self.assertEqual(models['models']['glm_AirlinesTrain_binary_1']['key'], 'glm_AirlinesTrain_binary_1', "key should equal our key: " + "glm_AirlinesTrain_binary_1")
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



    def test_binary_classifiers(self):
        node = h2o.nodes[0]

        print "##############################################"
        print "Testing /2/Models with scoring. . ."
        print "##############################################"
        print ""


        print "##############################################"
        print "Scoring compatible frames for compatible models for /2/Models?key=airlines_train.hex&find_compatible_models=true. . ."
        frames = node.frames(key='airlines_train.hex', find_compatible_models=1)
        compatible_models = frames['frames']['airlines_train.hex']['compatible_models']

        # NOTE: we start with frame airlines_train.hex and find the compatible models.
        # Then for each of those models we find all the compatible frames (there are at least two)
        # and score them.
        for model_key in compatible_models:
            # find all compatible frames
            models = node.models(key=model_key, find_compatible_frames=1)
            compatible_frames = models['models'][model_key]['compatible_frames']
            self.assertNotEqual(models['models'][model_key]['training_duration_in_ms'], 0, "Expected non-zero training time for model: " + model_key)

            for frame_key in compatible_frames:
                print "Scoring: /2/Models?key=" + model_key + "&score_frame=" + frame_key
                scoring_result = node.models(key=model_key, score_frame=frame_key)

                self.assertKeysExist(scoring_result, '', ['metrics'])
                self.assertKeysExist(scoring_result, 'metrics[0]', ['model', 'frame', 'duration_in_ms'])
                self.assertKeysExist(scoring_result, 'metrics[0]/model', ['key', 'model_category', 'id', 'creation_epoch_time_millis'])
                model_category = scoring_result['metrics'][0]['model']['model_category']
                self.assertEqual(scoring_result['metrics'][0]['model']['key'], model_key, "Expected model key: " + model_key + " but got: " + scoring_result['metrics'][0]['model']['key'])
                self.assertEqual(scoring_result['metrics'][0]['frame']['key'], frame_key, "Expected frame key: " + frame_key + " but got: " + scoring_result['metrics'][0]['frame']['key'])
                if model_category is 'Binomial':
                    self.assertKeysExist(scoring_result, 'metrics[0]', ['cm', 'auc']) # TODO: HitRatio
                # TODO: look inside the auc and cm elements
                if model_category is 'Regression':
                    self.assertKeysDontExist(scoring_result, 'metrics[0]', ['cm', 'auc']) # TODO: HitRatio


        print "##############################################"
        print "Testing /2/Frames with scoring. . ."
        print "##############################################"
        print ""


        print "##############################################"
        print "Scoring compatible models for /2/Frames?key=prostate.hex&find_compatible_models=true. . ."
        frames = node.frames(key='prostate.hex', find_compatible_models=1)
        compatible_models = frames['frames']['prostate.hex']['compatible_models']

        for model_key in compatible_models:
            print "Scoring: /2/Frames?key=prostate.hex&score_model=" + model_key
            scoring_result = node.frames(key='prostate.hex', score_model=model_key)

            self.assertKeysExist(scoring_result, '', ['metrics'])
            self.assertKeysExist(scoring_result, 'metrics[0]', ['model_category'])
            model_category = scoring_result['metrics'][0]['model_category']
            self.assertKeysExist(scoring_result, 'metrics[0]', ['model', 'frame', 'duration_in_ms'])
            self.assertEqual(scoring_result['metrics'][0]['model']['key'], model_key, "Expected model key: " + model_key + " but got: " + scoring_result['metrics'][0]['model']['key'])
            self.assertEqual(scoring_result['metrics'][0]['frame']['key'], 'prostate.hex', "Expected frame key: " + 'prostate.hex' + " but got: " + scoring_result['metrics'][0]['frame']['key'])
            if model_category is 'Binomial':
                self.assertKeysExist(scoring_result, 'metrics[0]', ['cm', 'auc']) # TODO: HitRatio
            # TODO: look inside the auc and cm elements
            if model_category is 'Regression':
                self.assertKeysDontExist(scoring_result, 'metrics[0]', ['cm', 'auc']) # TODO: HitRatio


    def test_steam(self):
        print "----------------------------------------------------------"
        print "                    Testing Steam...                      "
        print "----------------------------------------------------------"

        # Go up two dirs and add '/client'.
        # Don't know if there's a better way to do this. - Prithvi
        client_dir = os.path.join(os.path.split(os.path.split(os.path.dirname(os.path.realpath(__file__)))[0])[0], 'client')

        node0 = h2o.nodes[0]
        os.environ['STEAM_NODE_ADDR'] = node0.http_addr
        os.environ['STEAM_NODE_PORT'] = str(node0.port)

        # Run `make test -C path_to_h2o/client`
        command_string = "make test -C " + client_dir

        # Ideally there should have been some kind of exit code checking or exception handling here. 
        # However, when `make test` fails, h2o.spawn_wait() fails hard without an exit code. 
        # Further, if this is trapped in a try/except, the failed tests are not routed to stdout.
        (ps, outpath, errpath) =  h2o.spawn_cmd('steam_tests', command_string.split())
        h2o.spawn_wait(ps, outpath, errpath, timeout=1000)
        print "----------------------------------------------------------"
        print "            Steam tests completed successfully!           "
        print "----------------------------------------------------------"

if __name__ == '__main__':
    h2o.unit_main()
