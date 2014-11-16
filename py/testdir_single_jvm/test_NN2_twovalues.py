import unittest, time, sys, re
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_nn, h2o_cmd, h2o_browse as h2b, h2o_import as h2i, h2o_gbm

def write_syn_dataset(csvPathname, rowCount, rowDataTrue, rowDataFalse, outputTrue, outputFalse):
    dsf = open(csvPathname, "w+")
    for i in range(int(rowCount/2)):
        dsf.write(rowDataTrue + ',' + outputTrue + "\n")

    for i in range(int(rowCount/2)):
        dsf.write(rowDataFalse + ',' + outputFalse + "\n")
    dsf.close()

class test_NN_twovalues(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        # fails with 3
        global SEED
        SEED = h2o.setup_random_seed()
        h2o.init(1, java_heap_GB=4)
        # h2b.browseTheCloud()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud(h2o.nodes)
    
    def test_DeepLearning_twovalues(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        csvFilename = "syn_twovalues.csv"
        csvPathname = SYNDATASETS_DIR + '/' + csvFilename

        rowDataTrue    = "1, 0, 65, 1, 2, 1, 1, 4, 1, 4, 1, 4"
        rowDataFalse   = "0, 1, 0, -1, -2, -1, -1, -4, -1, -4, -1, -4" 

        twoValueList = [
            ('A','B',0, 14),
            ('A','B',1, 14),
            (0,1,0, 12),
            (0,1,1, 12),
            (0,1,'NaN', 12),
            (1,0,'NaN', 12),
            (-1,1,0, 12),
            (-1,1,1, 12),
            (-1e1,1e1,1e1, 12),
            (-1e1,1e1,-1e1, 12),
            ]

        trial = 0
        for (outputTrue, outputFalse, case, coeffNum) in twoValueList:
            write_syn_dataset(csvPathname, 20, 
                rowDataTrue, rowDataFalse, str(outputTrue), str(outputFalse))

            start = time.time()
            hex_key = csvFilename + "_" + str(trial)
            model_key = 'trial_' + str(trial) + '.hex'
            validation_key = hex_key

            parseResult = h2i.import_parse(path=csvPathname, schema='put', hex_key=hex_key)
            print "using outputTrue: %s outputFalse: %s" % (outputTrue, outputFalse)

            inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
            print "\n" + csvPathname, \
                "    numRows:", "{:,}".format(inspect['numRows']), \
                "    numCols:", "{:,}".format(inspect['numCols'])
            response = inspect['numCols']
            response = 'C' + str(response)

            kwargs = {
                'ignored_cols'                 : None,
                'response'                     : response,
                'classification'               : 1,
                'activation'                   : 'Tanh',
                #'input_dropout_ratio'          : 0.2,
                'hidden'                       : '113,71,54',
                'rate'                         : 0.01,
                'rate_annealing'               : 1e-6,
                'momentum_start'               : 0,
                'momentum_stable'              : 0,
                'l1'                           : 0.0,
                'l2'                           : 1e-6,
                'seed'                         : 80023842348,
                'loss'                         : 'CrossEntropy',
                #'max_w2'                       : 15,
                'initial_weight_distribution'  : 'UniformAdaptive',
                #'initial_weight_scale'         : 0.01,
                'epochs'                       : 100,
                'destination_key'              : model_key,
                'validation'                   : hex_key,
            }

            timeoutSecs = 60
            start = time.time()
            h2o_cmd.runDeepLearning(parseResult=parseResult, timeoutSecs=timeoutSecs, **kwargs)
            print "trial #", trial, "Deep Learning end on ", csvFilename, ' took', time.time() - start, 'seconds'

            #### Now score using the model, and check the validation error
            expectedErr = 0.00
            relTol = 0.01
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


            trial += 1

if __name__ == '__main__':
    h2o.unit_main()
