import unittest, time, sys, os, re
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_import as h2i, h2o_exec
import h2o_glm, h2o_gbm, h2o_rf

class ModelManagementTestCase(unittest.TestCase):
    tear_down_cloud = True
    # tear_down_cloud = False

    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):

        cloud_size = 5

        if h2o.clone_cloud_json != None:
            print "NOTE: Connecting to existing cloud, and leaving the cloud running afterwards: " + \
                os.path.abspath(h2o.clone_cloud_json)

        print "Calling h2o.init(" + str(cloud_size) + "). . ."
        h2o.init(cloud_size, java_heap_GB=2, timeoutSecs=120)
        
    @classmethod
    def tearDownClass(cls):
        if h2o.clone_cloud_json == None:
            if ModelManagementTestCase.tear_down_cloud:
                h2o.tear_down_cloud()
            else:
                None
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
        node = h2o.nodes[0]

        prostate_hex = self.import_frame('prostate.hex', 'smalldata', 'prostate.csv', 'logreg', 380, 9)
        airlines_train_hex = self.import_frame('airlines_train.hex', 'smalldata', 'AirlinesTrain.csv.zip', 'airlines', 24421, 12)
        airlines_test_hex = self.import_frame('airlines_test.hex', 'smalldata', 'AirlinesTest.csv.zip', 'airlines', 2691, 12)
        has_uuid_hex = self.import_frame('has_uuid.hex', 'smalldata', 'test_all_raw_top10rows.csv', 'test', 12, 89)

        # get the hashes
        print "Checking " + str(len(h2o.nodes)) + " nodes for frames: "
        for a_node in h2o.nodes:
            print "  " + a_node.http_addr + ":" + str(a_node.port)

        test_hash_before = -1
        train_hash_before = -1
        for a_node in h2o.nodes:
            print "  Checking " + a_node.http_addr + ":" + str(a_node.port)
            frames = a_node.frames()
            self.assertKeysExist(frames, 'frames', ['airlines_train.hex'])
            self.assertKeysExist(frames, 'frames', ['airlines_test.hex'])
            self.assertKeysExist(frames, 'frames/airlines_test.hex', ['id'])
            self.assertKeysExist(frames, 'frames', ['has_uuid.hex'])

            # Make sure we have the same checksums everywhere:
            tmp = frames['frames']['airlines_test.hex']['id']
            if test_hash_before != -1:
                self.assertEquals(tmp, test_hash_before, "Same hash on every node for airlines_test.hex")
            test_hash_before = tmp

            # Make sure we have the same checksums everywhere:
            tmp = frames['frames']['airlines_train.hex']['id']
            if train_hash_before != -1:
                self.assertEquals(tmp, train_hash_before, "Same hash on every node for airlines_train.hex")
            train_hash_before = tmp

            self.assertNotEqual("ffffffffffffffff", test_hash_before);
            self.assertNotEqual("ffffffffffffffff", train_hash_before);
            self.assertNotEqual("0", test_hash_before);
            self.assertNotEqual("0", train_hash_before);

        # Add new proper boolean response columns
        self.create_new_boolean('airlines_train.hex', 'IsDepDelayed_REC', 'IsDepDelayed_REC_recoded')
        self.create_new_boolean('airlines_test.hex', 'IsDepDelayed_REC', 'IsDepDelayed_REC_recoded')

        # get the hashes and ensure they've changed
        frames = node.frames()
        self.assertKeysExist(frames, 'frames', ['airlines_train.hex'])
        self.assertKeysExist(frames, 'frames', ['airlines_test.hex'])
        self.assertKeysExist(frames, 'frames/airlines_test.hex', ['id'])

        train_hash_after = frames['frames']['airlines_train.hex']['id']
        test_hash_after = frames['frames']['airlines_test.hex']['id']

        self.assertNotEqual(train_hash_before, train_hash_after, "Expected airlines_train hash to change. . .  Before and after were both: " + train_hash_after)
        self.assertNotEqual(test_hash_before, test_hash_after, "Expected airlines_test hash to change. . .  Before and after were both: " + test_hash_after)

        print "airlines_train hash before: ", train_hash_before, ", after: ", train_hash_after
        print "airlines_test hash before: ", test_hash_before, ", after: ", test_hash_after

        return (prostate_hex, airlines_train_hex, airlines_test_hex)
        

    def create_models(self, frame_keys):
        prostate_hex, airlines_train_hex, airlines_test_hex = frame_keys

        self.assertIsNotNone(prostate_hex)
        self.assertIsNotNone(airlines_train_hex)
        self.assertIsNotNone(airlines_test_hex)

        node = h2o.nodes[0]

        num_models = 0
        durations = {}

        print "##############################################################"
        print "Generating AirlinesTrain GLM2 binary classification model. . ."
        # R equivalent: h2o.glm.FV(y = "IsDepDelayed", x = c("Origin", "Dest", "fDayofMonth", "fYear", "UniqueCarrier", "fDayOfWeek", "fMonth", "DepTime", "ArrTime", "Distance"), data = airlines_train.hex, family = "binomial", alpha=0.05, lambda=1.0e-2, standardize=FALSE, nfolds=0)
        before = time.time() * 1000
        glm_AirlinesTrain_1_params = {
            'destination_key': 'glm_AirlinesTrain_binary_1',
            'response': 'IsDepDelayed', 
            'ignored_cols': 'IsDepDelayed_REC, IsDepDelayed_REC_recoded', 
            'family': 'binomial', 
            'alpha': 0.5, 
            'standardize': 0, 
            'lambda': 1.0e-2, 
            'n_folds': 0,
            'use_all_factor_levels': 1,
            'variable_importances': 1
        }
        glm_AirlinesTrain_1 = node.GLM(airlines_train_hex, **glm_AirlinesTrain_1_params)
        durations['glm_AirlinesTrain_binary_1'] = time.time() * 1000 - before
        num_models = num_models + 1
        h2o_glm.simpleCheckGLM(self, glm_AirlinesTrain_1, None, **glm_AirlinesTrain_1_params)

        for a_node in h2o.nodes:
            print "Checking /Frames and /Models on: " + a_node.http_addr + ":" + str(a_node.port)
            dummy = a_node.frames()
            dummy = a_node.models()

        print "#########################################################################################"
        print "Generating AirlinesTrain GLM2 binary classification model with nfold crossvalidation. . ."
        # R equivalent: h2o.glm.FV(y = "IsDepDelayed", x = c("Origin", "Dest", "fDayofMonth", "fYear", "UniqueCarrier", "fDayOfWeek", "fMonth", "DepTime", "ArrTime", "Distance"), data = airlines_train.hex, family = "binomial", alpha=0.05, lambda=1.0e-2, standardize=FALSE, nfolds=3)
        before = time.time() * 1000
        glm_AirlinesTrain_3fold_params = {
            'destination_key': 'glm_AirlinesTrain_binary_3fold',
            'response': 'IsDepDelayed', 
            'ignored_cols': 'IsDepDelayed_REC, IsDepDelayed_REC_recoded', 
            'family': 'binomial', 
            'alpha': 0.5, 
            'standardize': 0, 
            'lambda': 1.0e-2, 
            'n_folds': 3,
            'use_all_factor_levels': 1,
            'variable_importances': 1
        }
        glm_AirlinesTrain_3fold = node.GLM(airlines_train_hex, **glm_AirlinesTrain_3fold_params)
        durations['glm_AirlinesTrain_binary_3fold'] = time.time() * 1000 - before
        num_models = num_models + 1 # TODO: interesting that the xval models aren't visible as they are in GBM
        h2o_glm.simpleCheckGLM(self, glm_AirlinesTrain_3fold, None, **glm_AirlinesTrain_3fold_params)

        for a_node in h2o.nodes:
            print "Checking /Frames and /Models on: " + a_node.http_addr + ":" + str(a_node.port)
            dummy = a_node.frames()
            dummy = a_node.models()



        # print "##############################################################"
        # print "Grid search: Generating AirlinesTrain GLM2 binary classification models. . ."
        # before = time.time() * 1000
        # glm_AirlinesTrain_grid_params = {
        #         'destination_key': 'glm_AirlinesTrain_binary_grid_',
        #     'response': 'IsDepDelayed', 
        #     'ignored_cols': 'IsDepDelayed_REC, IsDepDelayed_REC_recoded', 
        #     'family': 'binomial', 
        #     'alpha': '0.5, 1.0', 
        #     'standardize': 0, 
        #     'lambda': '1.0e-2,1.0e-3,1.0e-4', 
        #     'n_folds': 2,
        #     'use_all_factor_levels': 1,
        #     'variable_importances': 1
        # }
        # glm_AirlinesTrain_grid = node.GLMGrid(airlines_train_hex, **glm_AirlinesTrain_grid_params)
        # durations['glm_AirlinesTrain_binary_grid'] = time.time() * 1000 - before
        # num_models = num_models + 6
        # h2o_glm.simpleCheckGLMGrid(self, glm_AirlinesTrain_grid, None, **glm_AirlinesTrain_grid_params)

        # for a_node in h2o.nodes:
        #     print "Checking /Frames and /Models on: " + a_node.http_addr + ":" + str(a_node.port)
        #     dummy = a_node.frames()
        #     dummy = a_node.models()




        print "####################################################################"
        print "Generating AirlinesTrain simple GBM binary classification model. . ."
        # R equivalent: h2o.gbm(y = "IsDepDelayed", x = c("Origin", "Dest", "fDayofMonth", "fYear", "UniqueCarrier", "fDayOfWeek", "fMonth", "DepTime", "ArrTime", "Distance"), data = airlines_train.hex, n.trees=3, interaction.depth=1, distribution="multinomial", n.minobsinnode=2, shrinkage=.1)
        before = time.time() * 1000
        gbm_AirlinesTrain_1_params = {
            'destination_key': 'gbm_AirlinesTrain_binary_1',
            'response': 'IsDepDelayed', 
            'ignored_cols_by_name': 'IsDepDelayed_REC, IsDepDelayed_REC_recoded', 
            'ntrees': 3,
            'max_depth': 1,
            'classification': 1,
            'n_folds': 0
            # TODO: what about minobsinnode and shrinkage?!
        }
        gbm_AirlinesTrain_1 = node.gbm(airlines_train_hex, **gbm_AirlinesTrain_1_params)
        durations['gbm_AirlinesTrain_binary_1'] = time.time() * 1000 - before
        num_models = num_models + 1

        for a_node in h2o.nodes:
            print "Checking /Frames and /Models on: " + a_node.http_addr + ":" + str(a_node.port)
            dummy = a_node.frames()
            dummy = a_node.models()

        print "#####################################################################"
        print "Generating AirlinesTrain complex GBM binary classification model. . ."
        # R equivalent: h2o.gbm(y = "IsDepDelayed", x = c("Origin", "Dest", "fDayofMonth", "fYear", "UniqueCarrier", "fDayOfWeek", "fMonth", "DepTime", "ArrTime", "Distance"), data = airlines_train.hex, n.trees=50, interaction.depth=5, distribution="multinomial", n.minobsinnode=2, shrinkage=.1)
        before = time.time() * 1000
        gbm_AirlinesTrain_2_params = {
            'destination_key': 'gbm_AirlinesTrain_binary_2',
            'response': 'IsDepDelayed', 
            'ignored_cols_by_name': 'IsDepDelayed_REC, IsDepDelayed_REC_recoded', 
            'ntrees': 50,
            'max_depth': 5,
            'classification': 1,
            'n_folds': 0
            # TODO: what about minobsinnode and shrinkage?!
        }
        gbm_AirlinesTrain_2 = node.gbm(airlines_train_hex, **gbm_AirlinesTrain_2_params)
        durations['gbm_AirlinesTrain_binary_2'] = time.time() * 1000 - before
        num_models = num_models + 1

        for a_node in h2o.nodes:
            print "Checking /Frames and /Models on: " + a_node.http_addr + ":" + str(a_node.port)
            dummy = a_node.frames()
            dummy = a_node.models()

        print "###############################################################################################"
        print "Generating AirlinesTrain simple GBM binary classification model with nfold crossvalidation. . ."
        # R equivalent: h2o.gbm(y = "IsDepDelayed", x = c("Origin", "Dest", "fDayofMonth", "fYear", "UniqueCarrier", "fDayOfWeek", "fMonth", "DepTime", "ArrTime", "Distance"), data = airlines_train.hex, n.trees=3, interaction.depth=1, distribution="multinomial", n.minobsinnode=2, shrinkage=.1)
        before = time.time() * 1000
        gbm_AirlinesTrain_3fold_params = {
            'destination_key': 'gbm_AirlinesTrain_binary_3fold',
            'response': 'IsDepDelayed', 
            'ignored_cols_by_name': 'IsDepDelayed_REC, IsDepDelayed_REC_recoded', 
            'ntrees': 3,
            'max_depth': 1,
            'classification': 1,
            'n_folds': 3
            # TODO: what about minobsinnode and shrinkage?!
        }
        gbm_AirlinesTrain_3fold = node.gbm(airlines_train_hex, **gbm_AirlinesTrain_3fold_params)
        durations['gbm_AirlinesTrain_binary_3fold'] = time.time() * 1000 - before
        num_models = num_models + 4 # 1 main model and 3 xval models

        for a_node in h2o.nodes:
            print "Checking /Frames and /Models on: " + a_node.http_addr + ":" + str(a_node.port)
            dummy = a_node.frames()
            dummy = a_node.models()

        print "####################################################################"
        print "Generating AirlinesTrain simple DRF binary classification model. . ."
        # R equivalent: h2o.randomForest.FV(y = "IsDepDelayed", x = c("Origin", "Dest", "fDayofMonth", "fYear", "UniqueCarrier", "fDayOfWeek", "fMonth", "DepTime", "ArrTime", "Distance"), data = airlines_train.hex, ntree=5, depth=2)
        before = time.time() * 1000
        rf_AirlinesTrain_1_params = {
            'destination_key': 'rf_AirlinesTrain_binary_1',
            'response': 'IsDepDelayed', 
            'ignored_cols_by_name': 'IsDepDelayed_REC, IsDepDelayed_REC_recoded', 
            'ntrees': 5,
            'max_depth': 2,
            'classification': 1,
            'seed': 1234567890123456789L
        }
        rf_AirlinesTrain_1 = node.random_forest(airlines_train_hex, **rf_AirlinesTrain_1_params)
        durations['rf_AirlinesTrain_binary_1'] = time.time() * 1000 - before
        num_models = num_models + 1

        for a_node in h2o.nodes:
            print "Checking /Frames and /Models on: " + a_node.http_addr + ":" + str(a_node.port)
            dummy = a_node.frames()
            dummy = a_node.models()

        print "#####################################################################"
        print "Generating AirlinesTrain complex DRF binary classification model. . ."
        # R equivalent: h2o.randomForest.FV(y = "IsDepDelayed", x = c("Origin", "Dest", "fDayofMonth", "fYear", "UniqueCarrier", "fDayOfWeek", "fMonth", "DepTime", "ArrTime", "Distance"), data = airlines_train.hex, ntree=50, depth=10)
        before = time.time() * 1000
        rf_AirlinesTrain_2_params = {
            'destination_key': 'rf_AirlinesTrain_binary_2',
            'response': 'IsDepDelayed', 
            'ignored_cols_by_name': 'IsDepDelayed_REC, IsDepDelayed_REC_recoded', 
            'ntrees': 50,
            'max_depth': 10,
            'classification': 1,
            'seed': 1234567890123456789L
        }
        rf_AirlinesTrain_2 = node.random_forest(airlines_train_hex, **rf_AirlinesTrain_2_params)
        durations['rf_AirlinesTrain_binary_2'] = time.time() * 1000 - before
        num_models = num_models + 1

        for a_node in h2o.nodes:
            print "Checking /Frames and /Models on: " + a_node.http_addr + ":" + str(a_node.port)
            dummy = a_node.frames()
            dummy = a_node.models()

        print "###############################################################################################"
        print "Generating AirlinesTrain simple DRF binary classification model with nfold crossvalidation. . ."
        # R equivalent: h2o.randomForest.FV(y = "IsDepDelayed", x = c("Origin", "Dest", "fDayofMonth", "fYear", "UniqueCarrier", "fDayOfWeek", "fMonth", "DepTime", "ArrTime", "Distance"), data = airlines_train.hex, ntree=5, depth=2)
        before = time.time() * 1000
        rf_AirlinesTrain_3fold_params = {
            'destination_key': 'rf_AirlinesTrain_binary_3fold',
            'response': 'IsDepDelayed', 
            'ignored_cols_by_name': 'IsDepDelayed_REC, IsDepDelayed_REC_recoded', 
            'ntrees': 5,
            'max_depth': 2,
            'classification': 1,
            'seed': 1234567890123456789L,
            'n_folds': 3
        }
        rf_AirlinesTrain_3fold = node.random_forest(airlines_train_hex, **rf_AirlinesTrain_3fold_params)
        durations['rf_AirlinesTrain_binary_3fold'] = time.time() * 1000 - before
        num_models = num_models + 4

        for a_node in h2o.nodes:
            print "Checking /Frames and /Models on: " + a_node.http_addr + ":" + str(a_node.port)
            dummy = a_node.frames()
            dummy = a_node.models()

        print "#####################################################################"
        print "Generating AirlinesTrain complex SpeeDRF binary classification model. . ."
        # what is the R binding?
        before = time.time() * 1000
        speedrf_AirlinesTrain_1_params = {
            'destination_key': 'speedrf_AirlinesTrain_binary_1',
            'response': 'IsDepDelayed', 
            'ignored_cols_by_name': 'IsDepDelayed_REC, IsDepDelayed_REC_recoded', 
            'ntrees': 50,
            'max_depth': 10,
            'classification': 1,
            'importance': 1,
            'seed': 1234567890123456789L
        }
        speedrf_AirlinesTrain_1 = node.speedrf(airlines_train_hex, **speedrf_AirlinesTrain_1_params)
        durations['speedrf_AirlinesTrain_binary_1'] = time.time() * 1000 - before
        num_models = num_models + 1

        for a_node in h2o.nodes:
            print "Checking /Frames and /Models on: " + a_node.http_addr + ":" + str(a_node.port)
            dummy = a_node.frames()
            dummy = a_node.models()

        print "####################################################################################################"
        print "Generating AirlinesTrain complex SpeeDRF binary classification model with nfold crossvalidation. . ."
        # what is the R binding?
        before = time.time() * 1000
        speedrf_AirlinesTrain_3fold_params = {
            'destination_key': 'speedrf_AirlinesTrain_binary_3fold',
            'response': 'IsDepDelayed', 
            'ignored_cols_by_name': 'IsDepDelayed_REC, IsDepDelayed_REC_recoded', 
            'ntrees': 50,
            'max_depth': 10,
            'classification': 1,
            'importance': 1,
            'seed': 1234567890123456789L,
            'n_folds': 3
        }
        speedrf_AirlinesTrain_3fold = node.speedrf(airlines_train_hex, **speedrf_AirlinesTrain_3fold_params)
        durations['speedrf_AirlinesTrain_binary_3fold'] = time.time() * 1000 - before
        num_models = num_models + 4 # 1 main model and 3 xval models

        for a_node in h2o.nodes:
            print "Checking /Frames and /Models on: " + a_node.http_addr + ":" + str(a_node.port)
            dummy = a_node.frames()
            dummy = a_node.models()

        print "######################################################################"
        print "Generating AirlinesTrain DeepLearning binary classification model. . ."
        # R equivalent: h2o.deeplearning(y = "IsDepDelayed", x = c("Origin", "Dest", "fDayofMonth", "fYear", "UniqueCarrier", "fDayOfWeek", "fMonth", "DepTime", "ArrTime", "Distance"), data = airlines_train.hex, classification=TRUE, hidden=c(10, 10))
        before = time.time() * 1000
        dl_AirlinesTrain_1_params = {
            'destination_key': 'dl_AirlinesTrain_binary_1',
            'response': 'IsDepDelayed', 
            'ignored_cols': 'IsDepDelayed_REC, IsDepDelayed_REC_recoded', 
            'hidden': [10, 10],
            'classification': 1,
            'variable_importances': 1
        }
        dl_AirlinesTrain_1 = node.deep_learning(airlines_train_hex, **dl_AirlinesTrain_1_params)
        durations['dl_AirlinesTrain_binary_1'] = time.time() * 1000 - before
        num_models = num_models + 1

        for a_node in h2o.nodes:
            print "Checking /Frames and /Models on: " + a_node.http_addr + ":" + str(a_node.port)
            dummy = a_node.frames()
            dummy = a_node.models()

        print "##############################################################################################"
        print "Generating AirlinesTrain GLM2 binary classification model with different response column. . ."
        # R equivalent: h2o.glm.FV(y = "IsDepDelayed_REC", x = c("Origin", "Dest", "fDayofMonth", "fYear", "UniqueCarrier", "fDayOfWeek", "fMonth", "DepTime", "ArrTime", "Distance"), data = airlines_train.hex, family = "binomial", alpha=0.05, lambda=1.0e-2, standardize=FALSE, nfolds=0)
        before = time.time() * 1000
        glm_AirlinesTrain_A_params = {
            'destination_key': 'glm_AirlinesTrain_binary_A',
            'response': 'IsDepDelayed_REC_recoded', 
            'ignored_cols': 'IsDepDelayed, IsDepDelayed_REC', 
            'family': 'binomial', 
            'alpha': 0.5, 
            'standardize': 0, 
            'lambda': 1.0e-2, 
            'n_folds': 0,
            'use_all_factor_levels': 1,
            'variable_importances': 1
        }
        glm_AirlinesTrain_A = node.GLM(airlines_train_hex, **glm_AirlinesTrain_A_params)
        durations['glm_AirlinesTrain_binary_A'] = time.time() * 1000 - before
        num_models = num_models + 1
        h2o_glm.simpleCheckGLM(self, glm_AirlinesTrain_A, None, **glm_AirlinesTrain_A_params)

        for a_node in h2o.nodes:
            print "Checking /Frames and /Models on: " + a_node.http_addr + ":" + str(a_node.port)
            dummy = a_node.frames()
            dummy = a_node.models()

        print "#################################################################################################"
        print "Generating AirlinesTrain DeepLearning binary classification model with nfold crossvalidation. . ."
        # R equivalent: h2o.deeplearning(y = "IsDepDelayed", x = c("Origin", "Dest", "fDayofMonth", "fYear", "UniqueCarrier", "fDayOfWeek", "fMonth", "DepTime", "ArrTime", "Distance"), data = airlines_train.hex, classification=TRUE, hidden=c(10, 10), nfolds=3)
        before = time.time() * 1000
        dl_AirlinesTrain_3fold_params = {
            'destination_key': 'dl_AirlinesTrain_binary_3fold',
            'response': 'IsDepDelayed', 
            'ignored_cols': 'IsDepDelayed_REC, IsDepDelayed_REC_recoded', 
            'hidden': [10, 10],
            'classification': 1,
            'variable_importances': 1,
            'n_folds': 3
        }
        dl_AirlinesTrain_3fold = node.deep_learning(airlines_train_hex, **dl_AirlinesTrain_3fold_params)
        durations['dl_AirlinesTrain_binary_3fold'] = time.time() * 1000 - before
        num_models = num_models + 4 # 1 main model and 3 xval models

        for a_node in h2o.nodes:
            print "Checking /Frames and /Models on: " + a_node.http_addr + ":" + str(a_node.port)
            dummy = a_node.frames()
            dummy = a_node.models()

        print "##############################################################################################"
        print "Generating AirlinesTrain Naive Bayes binary classification model. . ."
        # R equivalent: h2o.naive_bayes(y = "IsDepDelayed", x = c("Origin", "Dest", "fDayofMonth", "fYear", "UniqueCarrier", "fDayOfWeek", "fMonth", "DepTime", "ArrTime", "Distance"), data = airlines_train.hex, family = "binomial", alpha=0.05, lambda=1.0e-2, standardize=FALSE, nfolds=0)
        before = time.time() * 1000
        nb_AirlinesTrain_params = {
            'destination_key': 'nb_AirlinesTrain_binary_1',
            'response': 'IsDepDelayed', 
            'ignored_cols': 'IsDepDelayed_REC_recoded, IsDepDelayed_REC', 
        }
        nb_AirlinesTrain = node.naive_bayes(source=airlines_train_hex, timeoutSecs=120, **nb_AirlinesTrain_params)
        durations['nb_AirlinesTrain_binary_1'] = time.time() * 1000 - before
        num_models = num_models + 1

        for a_node in h2o.nodes:
            print "Checking /Frames and /Models on: " + a_node.http_addr + ":" + str(a_node.port)
            dummy = a_node.frames()
            dummy = a_node.models()

        # These Prostate GLM models are also used to test that we get a warning only if variable_importances == 1 and use_all_factor_levels = 0.  The defaults for these are now both 0.  There are 9 combinations to test:
        # num	variable_importances	use_all_factor_levels	warning expected?
        # -----------------------------------------------------------------------
        # 00	0			0			False
        # 01	0			1			False
        # 10	1			0			True
        # 11	1			1			False
        # xx	default (0)		default (0)		False
        # x0	default (0)		0			False
        # x1	default (0)		1			False
        # 0x	0			default (0)		False
        # 1x	1			default (0)		True

        print "#########################################################"
        print "Generating Prostate GLM2 binary classification model with variable_importances false and use_all_factor_levels false (should have no warnings) . . ."
        # R equivalent: h2o.glm.FV(y = "CAPSULE", x = c("AGE","RACE","PSA","DCAPS"), data = prostate.hex, family = "binomial", nfolds = 0, alpha = 0.5)
        before = time.time() * 1000
        glm_Prostate_00_params = {
            'destination_key': 'glm_Prostate_binary_00',
            'response': 'CAPSULE', 
            'ignored_cols': None, 
            'family': 'binomial', 
            'alpha': 0.5, 
            'n_folds': 0,
            'variable_importances': 0,
            'use_all_factor_levels': 0
        }
        glm_Prostate_00 = node.GLM(prostate_hex, **glm_Prostate_00_params)
        durations['glm_Prostate_binary_00'] = time.time() * 1000 - before
        num_models = num_models + 1
        h2o_glm.simpleCheckGLM(self, glm_Prostate_00, None, **glm_Prostate_00_params)

        for a_node in h2o.nodes:
            print "Checking /Frames and /Models on: " + a_node.http_addr + ":" + str(a_node.port)
            dummy = a_node.frames()
            dummy = a_node.models()

        print "#########################################################"
        print "Generating Prostate GLM2 binary classification model with variable_importances false and use_all_factor_levels true (should have no warnings) . . ."
        # R equivalent: h2o.glm.FV(y = "CAPSULE", x = c("AGE","RACE","PSA","DCAPS"), data = prostate.hex, family = "binomial", nfolds = 0, alpha = 0.5)
        before = time.time() * 1000
        glm_Prostate_01_params = {
            'destination_key': 'glm_Prostate_binary_01',
            'response': 'CAPSULE', 
            'ignored_cols': None, 
            'family': 'binomial', 
            'alpha': 0.5, 
            'n_folds': 0,
            'variable_importances': 0,
            'use_all_factor_levels': 1
        }
        glm_Prostate_01 = node.GLM(prostate_hex, **glm_Prostate_01_params)
        durations['glm_Prostate_binary_01'] = time.time() * 1000 - before
        num_models = num_models + 1
        h2o_glm.simpleCheckGLM(self, glm_Prostate_01, None, **glm_Prostate_01_params)

        for a_node in h2o.nodes:
            print "Checking /Frames and /Models on: " + a_node.http_addr + ":" + str(a_node.port)
            dummy = a_node.frames()
            dummy = a_node.models()

        print "#########################################################"
        print "Generating Prostate GLM2 binary classification model with variable_importances true and use_all_factor_levels false (should have a warning) . . ."
        # R equivalent: h2o.glm.FV(y = "CAPSULE", x = c("AGE","RACE","PSA","DCAPS"), data = prostate.hex, family = "binomial", nfolds = 0, alpha = 0.5)
        before = time.time() * 1000
        glm_Prostate_10_params = {
            'destination_key': 'glm_Prostate_binary_10',
            'response': 'CAPSULE', 
            'ignored_cols': None, 
            'family': 'binomial', 
            'alpha': 0.5, 
            'n_folds': 0,
            'variable_importances': 1,
            'use_all_factor_levels': 0
        }
        glm_Prostate_10 = node.GLM(prostate_hex, **glm_Prostate_10_params)
        durations['glm_Prostate_binary_10'] = time.time() * 1000 - before
        num_models = num_models + 1
        h2o_glm.simpleCheckGLM(self, glm_Prostate_10, None, **glm_Prostate_10_params)

        for a_node in h2o.nodes:
            print "Checking /Frames and /Models on: " + a_node.http_addr + ":" + str(a_node.port)
            dummy = a_node.frames()
            dummy = a_node.models()

        print "#########################################################"
        print "Generating Prostate GLM2 binary classification model with variable_importances true and use_all_factor_levels true (should have no warnings) . . ."
        # R equivalent: h2o.glm.FV(y = "CAPSULE", x = c("AGE","RACE","PSA","DCAPS"), data = prostate.hex, family = "binomial", nfolds = 0, alpha = 0.5)
        before = time.time() * 1000
        glm_Prostate_11_params = {
            'destination_key': 'glm_Prostate_binary_11',
            'response': 'CAPSULE', 
            'ignored_cols': None, 
            'family': 'binomial', 
            'alpha': 0.5, 
            'n_folds': 0,
            'variable_importances': 1,
            'use_all_factor_levels': 1
        }
        glm_Prostate_11 = node.GLM(prostate_hex, **glm_Prostate_11_params)
        durations['glm_Prostate_binary_11'] = time.time() * 1000 - before
        num_models = num_models + 1
        h2o_glm.simpleCheckGLM(self, glm_Prostate_11, None, **glm_Prostate_11_params)

        for a_node in h2o.nodes:
            print "Checking /Frames and /Models on: " + a_node.http_addr + ":" + str(a_node.port)
            dummy = a_node.frames()
            dummy = a_node.models()

        print "#########################################################"
        print "Generating Prostate GLM2 binary classification model with variable_importances default (should default to false) and use_all_factor_levels default (should default to false), should have no warnings . . ."
        # R equivalent: h2o.glm.FV(y = "CAPSULE", x = c("AGE","RACE","PSA","DCAPS"), data = prostate.hex, family = "binomial", nfolds = 0, alpha = 0.5)
        before = time.time() * 1000
        glm_Prostate_xx_params = {
            'destination_key': 'glm_Prostate_binary_xx',
            'response': 'CAPSULE', 
            'ignored_cols': None, 
            'family': 'binomial', 
            'alpha': 0.5, 
            'n_folds': 0,
            # 'variable_importances': 0,
            # 'use_all_factor_levels': 0
        }
        glm_Prostate_xx = node.GLM(prostate_hex, **glm_Prostate_xx_params)
        durations['glm_Prostate_binary_xx'] = time.time() * 1000 - before
        num_models = num_models + 1
        h2o_glm.simpleCheckGLM(self, glm_Prostate_xx, None, **glm_Prostate_xx_params)

        for a_node in h2o.nodes:
            print "Checking /Frames and /Models on: " + a_node.http_addr + ":" + str(a_node.port)
            dummy = a_node.frames()
            dummy = a_node.models()

        print "#########################################################"
        print "Generating Prostate GLM2 binary classification model with variable_importances default (should default to false) and use_all_factor_levels false (should have no warnings) . . ."
        # R equivalent: h2o.glm.FV(y = "CAPSULE", x = c("AGE","RACE","PSA","DCAPS"), data = prostate.hex, family = "binomial", nfolds = 0, alpha = 0.5)
        before = time.time() * 1000
        glm_Prostate_x0_params = {
            'destination_key': 'glm_Prostate_binary_x0',
            'response': 'CAPSULE', 
            'ignored_cols': None, 
            'family': 'binomial', 
            'alpha': 0.5, 
            'n_folds': 0,
            # 'variable_importances': 0,
            'use_all_factor_levels': 0
        }
        glm_Prostate_x0 = node.GLM(prostate_hex, **glm_Prostate_x0_params)
        durations['glm_Prostate_binary_x0'] = time.time() * 1000 - before
        num_models = num_models + 1
        h2o_glm.simpleCheckGLM(self, glm_Prostate_x0, None, **glm_Prostate_x0_params)

        for a_node in h2o.nodes:
            print "Checking /Frames and /Models on: " + a_node.http_addr + ":" + str(a_node.port)
            dummy = a_node.frames()
            dummy = a_node.models()

        print "#########################################################"
        print "Generating Prostate GLM2 binary classification model with variable_importances default (should default to false) and use_all_factor_levels true (should have no warnings) . . ."
        # R equivalent: h2o.glm.FV(y = "CAPSULE", x = c("AGE","RACE","PSA","DCAPS"), data = prostate.hex, family = "binomial", nfolds = 0, alpha = 0.5)
        before = time.time() * 1000
        glm_Prostate_x1_params = {
            'destination_key': 'glm_Prostate_binary_x1',
            'response': 'CAPSULE', 
            'ignored_cols': None, 
            'family': 'binomial', 
            'alpha': 0.5, 
            'n_folds': 0,
            # 'variable_importances': 0,
            'use_all_factor_levels': 1
        }
        glm_Prostate_x1 = node.GLM(prostate_hex, **glm_Prostate_x1_params)
        durations['glm_Prostate_binary_x1'] = time.time() * 1000 - before
        num_models = num_models + 1
        h2o_glm.simpleCheckGLM(self, glm_Prostate_x1, None, **glm_Prostate_x1_params)

        for a_node in h2o.nodes:
            print "Checking /Frames and /Models on: " + a_node.http_addr + ":" + str(a_node.port)
            dummy = a_node.frames()
            dummy = a_node.models()

        print "#########################################################"
        print "Generating Prostate GLM2 binary classification model with variable_importances false and use_all_factor_levels default (should default to false), should have no warnings . . ."
        # R equivalent: h2o.glm.FV(y = "CAPSULE", x = c("AGE","RACE","PSA","DCAPS"), data = prostate.hex, family = "binomial", nfolds = 0, alpha = 0.5)
        before = time.time() * 1000
        glm_Prostate_0x_params = {
            'destination_key': 'glm_Prostate_binary_0x',
            'response': 'CAPSULE', 
            'ignored_cols': None, 
            'family': 'binomial', 
            'alpha': 0.5, 
            'n_folds': 0,
            'variable_importances': 0,
            # 'use_all_factor_levels': 0
        }
        glm_Prostate_0x = node.GLM(prostate_hex, **glm_Prostate_0x_params)
        durations['glm_Prostate_binary_0x'] = time.time() * 1000 - before
        num_models = num_models + 1
        h2o_glm.simpleCheckGLM(self, glm_Prostate_0x, None, **glm_Prostate_0x_params)

        for a_node in h2o.nodes:
            print "Checking /Frames and /Models on: " + a_node.http_addr + ":" + str(a_node.port)
            dummy = a_node.frames()
            dummy = a_node.models()

        print "#########################################################"
        print "Generating Prostate GLM2 binary classification model with variable_importances True and use_all_factor_levels default (should default to false), should have a warning . . ."
        # R equivalent: h2o.glm.FV(y = "CAPSULE", x = c("AGE","RACE","PSA","DCAPS"), data = prostate.hex, family = "binomial", nfolds = 0, alpha = 0.5)
        before = time.time() * 1000
        glm_Prostate_1x_params = {
            'destination_key': 'glm_Prostate_binary_1x',
            'response': 'CAPSULE', 
            'ignored_cols': None, 
            'family': 'binomial', 
            'alpha': 0.5, 
            'n_folds': 0,
            'variable_importances': 1,
            # 'use_all_factor_levels': 0
        }
        glm_Prostate_1x = node.GLM(prostate_hex, **glm_Prostate_1x_params)
        durations['glm_Prostate_binary_1x'] = time.time() * 1000 - before
        num_models = num_models + 1
        h2o_glm.simpleCheckGLM(self, glm_Prostate_1x, None, **glm_Prostate_1x_params)

        for a_node in h2o.nodes:
            print "Checking /Frames and /Models on: " + a_node.http_addr + ":" + str(a_node.port)
            dummy = a_node.frames()
            dummy = a_node.models()

        #
        # END OF 9 PROSTATE GLM2 VARIATIONS
        #


        print "###############################################################"
        print "Generating Prostate simple DRF binary classification model. . ."
        # R equivalent: h2o.randomForest.FV(y = "CAPSULE", x = c("AGE","RACE","DCAPS"), data = prostate.hex, ntree=10, depth=5)
        before = time.time() * 1000
        rf_Prostate_1_params = {
            'destination_key': 'rf_Prostate_binary_1',
            'response': 'CAPSULE', 
            'ignored_cols_by_name': None, 
            'ntrees': 10,
            'max_depth': 5,
            'classification': 1,
            'seed': 1234567890123456789L
        }
        rf_Prostate_1 = node.random_forest(prostate_hex, **rf_Prostate_1_params)
        durations['rf_Prostate_binary_1'] = time.time() * 1000 - before
        num_models = num_models + 1

        for a_node in h2o.nodes:
            print "Checking /Frames and /Models on: " + a_node.http_addr + ":" + str(a_node.port)
            dummy = a_node.frames()
            dummy = a_node.models()

        print "#####################################################################"
        print "Generating Prostate complex SpeeDRF binary classification model. . ."
        before = time.time() * 1000
        speedrf_Prostate_1_params = {
            'destination_key': 'speedrf_Prostate_binary_1',
            'response': 'CAPSULE', 
            'ignored_cols_by_name': None, 
            'ntrees': 50,
            'max_depth': 10,
            'classification': 1,
            'importance': 1,
            'seed': 1234567890123456789L
        }
        speedrf_Prostate_1 = node.speedrf(prostate_hex, **speedrf_Prostate_1_params)
        num_models = num_models + 1
        durations['speedrf_Prostate_binary_1'] = time.time() * 1000 - before

        for a_node in h2o.nodes:
            print "Checking /Frames and /Models on: " + a_node.http_addr + ":" + str(a_node.port)
            dummy = a_node.frames()
            dummy = a_node.models()

        print "##############################################"
        print "Generating Prostate GLM2 regression model. . ."
        # R equivalent: h2o.glm.FV(y = "AGE", x = c("CAPSULE","RACE","PSA","DCAPS"), data = prostate.hex, family = "gaussian", nfolds = 0, alpha = 0.5)
        before = time.time() * 1000
        glm_Prostate_regression_1_params = {
            'destination_key': 'glm_Prostate_regression_1',
            'response': 'AGE', 
            'ignored_cols': None, 
            'family': 'gaussian', 
            'alpha': 0.5, 
            'n_folds': 0,
            'use_all_factor_levels': 1,
            'variable_importances': 1
        }
        glm_Prostate_regression_1 = node.GLM(prostate_hex, **glm_Prostate_regression_1_params)
        durations['glm_Prostate_regression_1'] = time.time() * 1000 - before
        num_models = num_models + 1
        h2o_glm.simpleCheckGLM(self, glm_Prostate_regression_1, None, **glm_Prostate_regression_1_params)

        for a_node in h2o.nodes:
            print "Checking /Frames and /Models on: " + a_node.http_addr + ":" + str(a_node.port)
            dummy = a_node.frames()
            dummy = a_node.models()
        
        # Done building models!
        # We were getting different results for each node.  Bad, bad bad. . .
        print "########################################################"
        print "Checking " + str(len(h2o.nodes)) + " nodes for " + str(num_models) + " models: "
        for a_node in h2o.nodes:
            print "  " + a_node.http_addr + ":" + str(a_node.port)

        found_problem = False
        for a_node in h2o.nodes:
            print "  Checking: " + a_node.http_addr + ":" + str(a_node.port)
            models = a_node.models()
            got = len(models['models'])
            print "For node: " + a_node.http_addr + ":" + str(a_node.port) + " checking that we got ", str(num_models), " models. . ."
            if num_models != got:
                print "p00p, not enough. . ."
                found_problem = True
                print "Got these models: " + repr(models['models'].keys())
                print "Expected " + str(num_models) + ", got: " + str(got)

            for key, value in models['models'].iteritems():
                self.assertEquals(value['state'], 'DONE', "Expected state to be DONE for model: " + key)
                idx = key.find('_xval')
                # For cross-validation models use the time for the parent model, since we should be less
                if -1 == idx:
                    expected = durations[key]
                else:
                    expected = durations[key[0:idx]]
                self.assertTrue(value['training_duration_in_ms'] < expected, "Expected training duration as computed by the server (" + str(value['training_duration_in_ms']) + ") to be less than we compute in the test  (" + str(expected) + ") for model: " + key)

                self.assertKeysExistAndNonNull(value, "", ['expert_parameters'])
                # TODO: put back when Long serialization is fixed (probably not until h2o-dev)
                # if 'seed' in value['expert_parameters']:
                #     self.assertEquals(long(value['expert_parameters']['seed']), 1234567890123456789L, "Seed incorrect for model: " + key + ".  Expected: 1234567890123456789; got: " + str(long(value['expert_parameters']['seed'])))
                # if '_seed' in value['expert_parameters']:
                #     self.assertEquals(long(value['expert_parameters']['_seed']), 1234567890123456789L, "Seed incorrect for model: " + key + ".  Expected: 1234567890123456789; got: " + str(long(value['expert_parameters']['_seed'])))
        self.assertNotEqual(found_problem, True, "Missing models on at least one node.")


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

    def assertKeysExistAndNonNull(self, d, path, keys):
        path_elems = path.split("/")

        d = self.followPath(d, path_elems)
        for key in keys:
            assert key in d, "Failed to find key: " + key + " in dict: " + repr(d)
            assert d[key] != None, "Value unexpectedly null: " + key + " in dict: " + repr(d)

    def assertKeysDontExist(self, d, path, keys):
        path_elems = path.split("/")

        d = self.followPath(d, path_elems)
        for key in keys:
            assert key not in d, "Unexpectedly found key: " + key + " in dict: " + repr(d)


    # TODO: look more inside the auc and cm elements
    def validate_binomial_classifier_metrics(self, metrics, model, frame):
        self.assertKeysExistAndNonNull(metrics, "", ['cm', 'auc', 'model', 'model_category', 'frame', 'duration_in_ms', 'scoring_time']) # TODO: HitRatio

        # test auc object
        self.assertNotEqual(None, metrics['auc'])

        # What fields should we find in the AUC object?  Well. . . the criteria:
        criteria = ['F1', 'F2', 'F0point5', 'accuracy', 'precision', 'recall', 'specificity', 'mcc', 'max_per_class_error']
        # And the "accuracy_for_criteria" and so on:
        criteria_max_min = [criterion + '_for_criteria' for criterion in criteria ]

        # And now hackage, because the error field is called "errorr" due to limitations of R:
        criteria += ['errorr']
        criteria_max_min += ['error_for_criteria']

        # then a bunch of other fields:
        misc_fields = ['thresholds', 'threshold_criterion', 'actual_domain', 'AUC', 'Gini', 'confusion_matrices', 'threshold_criteria', 'threshold_for_criteria', 'confusion_matrix_for_criteria']
        self.assertKeysExistAndNonNull(metrics, "auc", criteria + criteria_max_min + misc_fields)

        # So far so good.  Now, what about what's in the fields?  First the criteria lists that contain a value for each threshold:
        assert type(metrics['auc']['thresholds']) is list, "thresholds value is a list."
        assert len(metrics['auc']['thresholds']) > 0, "thresholds value is a list of more than 0 elments."
        num_thresholds = len(metrics['auc']['thresholds'])

        for criterion in criteria:
            assert len(metrics['auc'][criterion]) == num_thresholds, criterion + " list is the same length as thresholds list."
            assert metrics['auc'][criterion][num_thresholds / 2] != 0.0, criterion + " list has a non-zero median element."


        # Now the criteria lists that contain a value for each criterion:
        assert type(metrics['auc']['threshold_criteria']) is list, "threshold_criteria value is a list."
        assert len(metrics['auc']['threshold_criteria']) > 0, "threshold_criteria value is a list of more than 0 elments."
        num_threshold_criteria = len(metrics['auc']['threshold_criteria'])

        # Are we testing all of them?  Note that the threshold criteria sections don't include error / errorrrrrrr
        assert num_threshold_criteria == len(criteria) - 1, "We are testing all the threshold criteria (test a)."
        assert num_threshold_criteria == len(criteria_max_min) - 1, "We are testing all the threshold criteria (test b)."

        for criterion_mm in criteria_max_min:
            assert len(metrics['auc'][criterion_mm]) == num_threshold_criteria, criterion_mm + " list is the same length as threshold_criteria list."
            assert metrics['auc'][criterion_mm][num_threshold_criteria / 2] != 0.0, criterion_mm + " list has a non-zero median element."

        # confusion_matrix_for_criteria:
        assert len(metrics['auc']['confusion_matrix_for_criteria']) == num_threshold_criteria, "confusion_matrix_for_criteria list is the same length as threshold_criteria list."
        assert type(metrics['auc']['confusion_matrix_for_criteria']) is list, "confusion_matrix_for_criteria is a list."
        assert type(metrics['auc']['confusion_matrix_for_criteria'][num_threshold_criteria / 2]) is list, "confusion_matrix_for_criteria is a list of lists."
        assert type(metrics['auc']['confusion_matrix_for_criteria'][num_threshold_criteria / 2][0]) is list, "confusion_matrix_for_criteria is a list of lists of lists."
        assert metrics['auc']['confusion_matrix_for_criteria'][num_threshold_criteria / 2][0][0] != 0.0, "confusion_matrix_for_criteria list has a non-zero median element."

        # test cm object
        self.assertNotEqual(None, metrics['cm'])
        self.assertKeysExistAndNonNull(metrics, "cm", ['actual_domain', 'predicted_domain', 'domain', 'cm', 'mse'])
        assert type(metrics['cm']['actual_domain']) is list, "actual_domain is a list."
        assert type(metrics['cm']['predicted_domain']) is list, "predicted_domain is a list."
        assert type(metrics['cm']['domain']) is list, "domain is a list."
        assert len(metrics['cm']['actual_domain']) > 0, "actual_domain is a list of more than 0 elements."
        assert len(metrics['cm']['predicted_domain']) > 0, "predicted_domain is a list of more than 0 elements."
        assert len(metrics['cm']['domain']) > 0, "domain is a list of more than 0 elements."

        assert type(metrics['cm']['cm']) is list, "cm is a list."
        assert type(metrics['cm']['cm'][0]) is list, "cm is a list of lists."
        assert sum(metrics['cm']['cm'][0]) > 0, "first domain of cm has at least one non-zero value."
        self.assertNotEqual(0.0, metrics['cm']['mse'])

        # misc fields
        self.assertEquals(metrics['model_category'], 'Binomial')
        # self.assertNotEqual(0, metrics['duration_in_ms']) # NOTE: it's possible, but unlikely, for this to be 0 legitimately
        self.assertNotEqual(0, metrics['scoring_time'])
        
        # check model fields
        assert type(metrics['model']) is dict, "model field is an object."
        self.assertKeysExistAndNonNull(metrics, 'model', ['key', 'creation_epoch_time_millis', 'id', 'model_category'])
        self.assertEquals(metrics['model']['model_category'], 'Binomial')
        self.assertEquals(metrics['model']['key'], model)
        self.assertNotEqual(0, metrics['model']['creation_epoch_time_millis'])
        assert re.compile("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}").match(metrics['model']['id'])
        
        # check frame fields
        assert type(metrics['frame']) is dict, "frame field is an object."
        self.assertKeysExistAndNonNull(metrics, 'frame', ['key', 'creation_epoch_time_millis', 'id'])
        self.assertEquals(metrics['frame']['key'], frame)
        self.assertNotEqual(0, metrics['frame']['creation_epoch_time_millis'])
        assert re.compile("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}").match(metrics['frame']['id'])


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
        self.assertKeysDontExist(frames, 'frames', ['glm_AirlinesTrain_binary_1', 'gbm_AirlinesTrain_binary_1', 'gbm_AirlinesTrain_binary_2', 'rf_AirlinesTrain_binary_1', 'rf_AirlinesTrain_binary_2', 'dl_AirlinesTrain_binary_1', 'glm_AirlinesTrain_binary_A', 'glm_Prostate_binary_xx', 'rf_Prostate_binary_1', 'glm_Prostate_regression_1'])
        self.assertKeysDontExist(frames, '', ['models'])


        print "##############################################"
        print "Testing /2/Frames?key=airlines_test.hex. . ."
        frames = node.frames(key='airlines_test.hex')
        self.assertKeysExist(frames, 'frames', ['airlines_test.hex'])
        self.assertKeysDontExist(frames, 'frames', ['glm_AirlinesTrain_binary_1', 'gbm_AirlinesTrain_binary_1', 'gbm_AirlinesTrain_binary_2', 'rf_AirlinesTrain_binary_1', 'rf_AirlinesTrain_binary_2', 'dl_AirlinesTrain_binary_1', 'glm_AirlinesTrain_binary_A', 'glm_Prostate_binary_xx', 'rf_Prostate_binary_1', 'glm_Prostate_regression_1', 'airlines_train.hex', 'prostate.hex'])
        self.assertKeysDontExist(frames, '', ['models'])
        self.assertKeysExist(frames, 'frames/airlines_test.hex', ['creation_epoch_time_millis', 'id', 'key', 'column_names', 'compatible_models'])
        self.assertEqual(frames['frames']['airlines_test.hex']['id'], "fffffffffffff38d", msg="The airlines_test.hex frame hash should be deterministic.  Expected fffffffffffff38d, got: " + frames['frames']['airlines_test.hex']['id'])
        self.assertEqual(frames['frames']['airlines_test.hex']['key'], "airlines_test.hex", msg="The airlines_test.hex key should be airlines_test.hex.")


        print "##############################################"
        print "Testing /2/Frames?key=airlines_test.hex&find_compatible_models=true. . ."
        frames = node.frames(key='airlines_test.hex', find_compatible_models=1)
        self.assertKeysExist(frames, 'frames', ['airlines_test.hex'])
        self.assertKeysDontExist(frames, 'frames', ['glm_AirlinesTrain_binary_1', 'gbm_AirlinesTrain_binary_1', 'gbm_AirlinesTrain_binary_2', 'rf_AirlinesTrain_binary_1', 'rf_AirlinesTrain_binary_2', 'dl_AirlinesTrain_binary_1', 'glm_AirlinesTrain_binary_A', 'glm_Prostate_binary_xx', 'rf_Prostate_binary_1', 'glm_Prostate_regression_1', 'airlines_train.hex', 'prostate.hex'])

        self.assertKeysExist(frames, '', ['models'])
        self.assertKeysExist(frames, 'models', ['glm_AirlinesTrain_binary_1', 'gbm_AirlinesTrain_binary_1', 'gbm_AirlinesTrain_binary_2', 'rf_AirlinesTrain_binary_1', 'rf_AirlinesTrain_binary_2', 'dl_AirlinesTrain_binary_1', 'glm_AirlinesTrain_binary_A'])
        self.assertKeysDontExist(frames, 'models', ['glm_Prostate_binary_xx', 'rf_Prostate_binary_1', 'glm_Prostate_regression_1', 'airlines_train.hex', 'airlines_train.hex', 'airlines_test.hex', 'prostate.hex'])


        print "##############################################"
        print "Testing /2/Frames with various options. . ."
        print "##############################################"
        print ""


        print "##############################################"
        print "Testing /2/Models list. . ."
        models = node.models()
        self.assertKeysExist(models, 'models', ['glm_AirlinesTrain_binary_1', 'gbm_AirlinesTrain_binary_1', 'gbm_AirlinesTrain_binary_2', 'rf_AirlinesTrain_binary_1', 'rf_AirlinesTrain_binary_2', 'dl_AirlinesTrain_binary_1', 'glm_AirlinesTrain_binary_A', 'glm_Prostate_binary_xx', 'rf_Prostate_binary_1', 'glm_Prostate_regression_1'])
        self.assertKeysExist(models, 'models/glm_AirlinesTrain_binary_1', ['id', 'key', 'creation_epoch_time_millis', 'model_category', 'state', 'input_column_names', 'response_column_name', 'critical_parameters', 'secondary_parameters', 'expert_parameters', 'compatible_frames', 'warnings'])
        self.assertEqual(0, len(models['models']['glm_AirlinesTrain_binary_1']['warnings']), msg="Expect no warnings for glm_AirlinesTrain_binary_1.")
        self.assertEqual(models['models']['glm_AirlinesTrain_binary_1']['key'], 'glm_AirlinesTrain_binary_1', "key should equal our key: " + "glm_AirlinesTrain_binary_1")
        self.assertKeysDontExist(models, 'models', ['airlines_train.hex', 'airlines_test.hex', 'prostate.hex'])
        self.assertKeysDontExist(models, '', ['frames'])


        print "##############################################"
        print "Testing /2/Models?key=rf_Prostate_binary_1. . ."
        models = node.models(key='rf_Prostate_binary_1')
        self.assertKeysExist(models, 'models', ['rf_Prostate_binary_1'])
        self.assertKeysExist(models, 'models/rf_Prostate_binary_1', ['warnings'])
        self.assertEqual(0, len(models['models']['rf_Prostate_binary_1']['warnings']), msg="Expect no warnings for rf_Prostate_binary_1.")
        self.assertKeysDontExist(models, 'models', ['airlines_train.hex', 'airlines_test.hex', 'prostate.hex', 'glm_AirlinesTrain_binary_1', 'gbm_AirlinesTrain_binary_1', 'gbm_AirlinesTrain_binary_2', 'rf_AirlinesTrain_binary_1', 'rf_AirlinesTrain_binary_2', 'dl_AirlinesTrain_binary_1', 'glm_AirlinesTrain_binary_A', 'glm_Prostate_binary_xx', 'glm_Prostate_regression_1'])
        self.assertKeysDontExist(models, '', ['frames'])


        print "##############################################"
        print "Testing /2/Models?key=rf_Prostate_binary_1&find_compatible_frames=true. . ."
        models = node.models(key='rf_Prostate_binary_1', find_compatible_frames=1)
        self.assertKeysExist(models, 'models', ['rf_Prostate_binary_1'])
        self.assertKeysDontExist(models, 'models', ['airlines_train.hex', 'airlines_test.hex', 'prostate.hex', 'glm_AirlinesTrain_binary_1', 'gbm_AirlinesTrain_binary_1', 'gbm_AirlinesTrain_binary_2', 'rf_AirlinesTrain_binary_1', 'rf_AirlinesTrain_binary_2', 'dl_AirlinesTrain_binary_1', 'glm_AirlinesTrain_binary_A', 'glm_Prostate_binary_xx', 'glm_Prostate_regression_1'])

        self.assertKeysExist(models, '', ['frames'])
        self.assertKeysExist(models, 'frames', ['prostate.hex'])
        self.assertKeysDontExist(models, 'frames', ['airlines_train.hex', 'airlines_test.hex'])


        print "##############################################"
        print "Testing /2/Models?key=glm_Prostate_binary_* variable importance warnings. . ."
        should_have_warnings = ['glm_Prostate_binary_10', 'glm_Prostate_binary_1x']
        should_not_have_warnings = ['glm_Prostate_binary_00', 'glm_Prostate_binary_01', 'glm_Prostate_binary_11', 'glm_Prostate_binary_xx', 'glm_Prostate_binary_x0', 'glm_Prostate_binary_x1', 'glm_Prostate_binary_0x']

        for m in should_have_warnings:
            models = node.models(key=m)
            self.assertKeysExist(models, 'models', [m])
            self.assertKeysExist(models, 'models/' + m, ['warnings'])
            self.assertEqual(1, len(models['models'][m]['warnings']), msg="Expect one warning for " + m + ": " + repr(models['models'][m]['warnings']))
            self.assertTrue("use_all_factor_levels" in models['models'][m]['warnings'][0], "Expect variable importances warning since we aren't using use_all_factor_levels.")

        for m in should_not_have_warnings:
            models = node.models(key=m)
            self.assertKeysExist(models, 'models', [m])
            self.assertKeysExist(models, 'models/' + m, ['warnings'])
            self.assertEqual(0, len(models['models'][m]['warnings']), msg="Expect zero warnings for " + m + ": " + repr(models['models'][m]['warnings']))


    def test_binary_classifiers(self):
        node = h2o.nodes[0]

        print "##############################################"
        print "Testing /2/Models with scoring. . ."
        print "##############################################"
        print ""


        print "##############################################"
        test_frames = ["prostate.hex", "airlines_train.hex"]

        for test_frame in test_frames:
            print "Scoring compatible frames for compatible models for /2/Models?key=" + test_frame + "&find_compatible_models=true. . ."
            frames = node.frames(key=test_frame, find_compatible_models=1)
            compatible_models = frames['frames'][test_frame]['compatible_models']

            # NOTE: we start with frame airlines_train.hex and find the compatible models.
            # Then for each of those models we find all the compatible frames (there are at least two)
            # and score them.
            for model_key in compatible_models:
                # find all compatible frames
                models = node.models(key=model_key, find_compatible_frames=1)
                compatible_frames = models['models'][model_key]['compatible_frames']
                self.assertKeysExist(models, 'models/' + model_key, ['training_duration_in_ms'])
                self.assertNotEqual(models['models'][model_key]['training_duration_in_ms'], 0, "Expected non-zero training time for model: " + model_key)

                should_not_have_varimp = ['glm_Prostate_binary_00', 'glm_Prostate_binary_01', 'glm_Prostate_binary_0x', 'glm_Prostate_binary_xx', 'glm_Prostate_binary_x0', 'glm_Prostate_binary_x1']
                if models['models'][model_key]['model_algorithm'] != 'Naive Bayes' and model_key not in should_not_have_varimp:
                    self.assertKeysExistAndNonNull(models, 'models/' + model_key, ['variable_importances'])
                    self.assertKeysExistAndNonNull(models, 'models/' + model_key + '/variable_importances', ['varimp', 'method', 'max_var', 'scaled'])

                for frame_key in compatible_frames:
                    print "Scoring: /2/Models?key=" + model_key + "&score_frame=" + frame_key
                    scoring_result = node.models(key=model_key, score_frame=frame_key)

                    self.assertKeysExist(scoring_result, '', ['metrics'])
                    self.assertKeysExist(scoring_result, 'metrics[0]', ['model', 'frame', 'duration_in_ms'])
                    self.assertKeysExist(scoring_result, 'metrics[0]/model', ['key', 'model_category', 'id', 'creation_epoch_time_millis'])
                    model_category = scoring_result['metrics'][0]['model']['model_category']
                    self.assertEqual(scoring_result['metrics'][0]['model']['key'], model_key, "Expected model key: " + model_key + " but got: " + scoring_result['metrics'][0]['model']['key'])
                    self.assertEqual(scoring_result['metrics'][0]['frame']['key'], frame_key, "Expected frame key: " + frame_key + " but got: " + scoring_result['metrics'][0]['frame']['key'])
                    if model_category == 'Binomial':
                        self.validate_binomial_classifier_metrics(scoring_result['metrics'][0], model_key, frame_key)

                    if model_category == 'Regression':
                        # self.assertKeysDontExist(scoring_result, 'metrics[0]', ['cm', 'auc']) # TODO: HitRatio
                        None


        print "##############################################"
        print "Testing /2/Frames with scoring. . ."
        print "##############################################"
        print ""


        print "##############################################"
        test_frames = ["prostate.hex", "airlines_test.hex"]

        for frame_key in test_frames:
            print "Scoring compatible models for /2/Frames?key=" + frame_key + "&find_compatible_models=true. . ."
            frames = node.frames(key=frame_key, find_compatible_models=1)
            compatible_models = frames['frames'][frame_key]['compatible_models']

            for model_key in compatible_models:
                print "Scoring: /2/Frames?key=" + frame_key + "&score_model=" + model_key
                scoring_result = node.frames(key=frame_key, score_model=model_key)

                self.assertKeysExist(scoring_result, '', ['metrics'])
                self.assertKeysExist(scoring_result, 'metrics[0]', ['model_category'])
                model_category = scoring_result['metrics'][0]['model_category']
                self.assertKeysExist(scoring_result, 'metrics[0]', ['model', 'frame', 'duration_in_ms'])
                self.assertEqual(scoring_result['metrics'][0]['model']['key'], model_key, "Expected model key: " + model_key + " but got: " + scoring_result['metrics'][0]['model']['key'])
                self.assertEqual(scoring_result['metrics'][0]['frame']['key'], frame_key, "Expected frame key: " + frame_key + " but got: " + scoring_result['metrics'][0]['frame']['key'])

                print "the model_category: ", model_category

                if model_category == 'Binomial':
                    self.validate_binomial_classifier_metrics(scoring_result['metrics'][0], model_key, frame_key)

                # TODO: look inside the auc and cm elements
                if model_category == 'Regression':
                    # self.assertKeysDontExist(scoring_result, 'metrics[0]', ['cm', 'auc']) # TODO: HitRatio
                    None


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


        # However, when `make test` fails, h2o.spawn_wait() fails hard without an exit code. 
        # Further, if this is trapped in a try/except, the failed tests are not routed to stdout.
        (ps, outpath, errpath) =  h2o.spawn_cmd('steam_tests', command_string.split())
        h2o.spawn_wait(ps, outpath, errpath, timeout=1000)
        print "----------------------------------------------------------"
        print "            Steam tests completed successfully!           "
        print "----------------------------------------------------------"

if __name__ == '__main__':
    h2o.unit_main()
