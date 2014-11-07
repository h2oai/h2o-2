import unittest, time, sys, random, string
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_nn, h2o_cmd, h2o_import as h2i

DO_SUMMARY=False

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = h2o.setup_random_seed()
        h2o.init(java_heap_GB=12)
            # requires user 0xcustomer to access c21 data. So needs to be run with a -cj *json, 
            # or as user 0xcustomer
            # or a config json for the user needs to exist in this directory (like pytest_config-jenkins.json)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_DeepLearning_c21(self):
        importFolderPath = '/mnt/0xcustomer-datasets/c21'
        csvPathname_train = importFolderPath + '/persona_clean_deep.tsv.zip'
        csvPathname_test  = importFolderPath + '/persona_clean_deep.tsv.zip'
        hex_key = 'train.hex'
        validation_key = 'test.hex'
        timeoutSecs = 300
        parseResult  = h2i.import_parse(path=csvPathname_train, hex_key=hex_key, 
            timeoutSecs=timeoutSecs, doSummary=DO_SUMMARY)
        parseResultV = h2i.import_parse(path=csvPathname_test, hex_key=validation_key, 
            timeoutSecs=timeoutSecs, doSummary=DO_SUMMARY)
        inspect = h2o_cmd.runInspect(None, hex_key)
        print "\n" + csvPathname_train, \
            "    numRows:", "{:,}".format(inspect['numRows']), \
            "    numCols:", "{:,}".format(inspect['numCols'])

        response = 'any_response'

        #Making random id
        identifier = ''.join(random.sample(string.ascii_lowercase + string.digits, 10))
        model_key = 'nn_' + identifier + '.hex'

        # use defaults otherwise
        # need to change epochs otherwise it takes too long
        kwargs = {
            'epochs'                       : 0.001,
            'response'                     : response,
            'destination_key'              : model_key,
            'validation'                   : validation_key,
        }
        ###expectedErr = 0.0362 ## from single-threaded mode
        expectedErr = 0.03 ## observed actual value with Hogwild

        timeoutSecs = 600
        start = time.time()
        nn = h2o_cmd.runDeepLearning(parseResult=parseResult, timeoutSecs=timeoutSecs, **kwargs)
        print "neural net end on ", csvPathname_train, " and ", csvPathname_test, 'took', time.time() - start, 'seconds'

        #### Now score using the model, and check the validation error
        expectedErr = 0.046
        relTol = 0.35 # allow 35% tolerance. kbn
        predict_key = 'Predict.hex'

        kwargs = {
            'data_key': validation_key,
            'destination_key': predict_key,
            'model_key': model_key
        }
        predictResult = h2o_cmd.runPredict(timeoutSecs=timeoutSecs, **kwargs)
        h2o_cmd.runInspect(key=predict_key, verbose=True)

        kwargs = {
        }

        predictCMResult = h2o.nodes[0].predict_confusion_matrix(
            actual=validation_key,
            vactual=response,
            predict=predict_key,
            vpredict='predict',
            timeoutSecs=timeoutSecs, **kwargs)

        cm = predictCMResult['cm']

        print h2o_gbm.pp_cm(cm)
        actualErr = h2o_gbm.pp_cm_summary(cm)/100.

        print "actual   classification error:" + format(actualErr)
        print "expected classification error:" + format(expectedErr)
        if actualErr != expectedErr and abs((expectedErr - actualErr)/expectedErr) > relTol:
            raise Exception("Scored classification error of %s is not within %s %% relative error of %s" %
                            (actualErr, float(relTol)*100, expectedErr))

if __name__ == '__main__':
    h2o.unit_main()
