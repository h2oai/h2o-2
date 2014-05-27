import unittest, time, sys, re
sys.path.extend(['.','..','py'])
import h2o, h2o_nn, h2o_cmd, h2o_hosts, h2o_browse as h2b, h2o_import as h2i

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
        global localhost
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(3, java_heap_GB=4)
        else:
            h2o_hosts.build_cloud_with_hosts(1)
        # h2b.browseTheCloud()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud(h2o.nodes)
    
    def test_NN_twovalues(self):
        h2o.beta_features = True
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

            parseResult = h2i.import_parse(path=csvPathname, schema='put', hex_key=hex_key)
            print "using outputTrue: %s outputFalse: %s" % (outputTrue, outputFalse)

            inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
            print "\n" + csvPathname, \
                "    numRows:", "{:,}".format(inspect['numRows']), \
                "    numCols:", "{:,}".format(inspect['numCols'])
            response = inspect['numCols'] - 1

            kwargs = {
                'ignored_cols'                 : None,
                'response'                     : 'C' + str(response),
                'classification'               : 1,
                'mode'                         : 'SingleThread',
                'activation'                   : 'Tanh',
                #'input_dropout_ratio'          : 0.2,
                'hidden'                       : '500',
                'rate'                         : 0.01,
                'rate_annealing'               : 1e-6,
                'momentum_start'               : 0,
                'momentum_ramp'                : 0,
                'momentum_stable'              : 0,
                'l1'                           : 0.0,
                'l2'                           : 1e-4,
                'seed'                         : 80023842348,
                'loss'                         : 'CrossEntropy',
                #'max_w2'                       : 15,
                #'warmup_samples'               : 0,
                'initial_weight_distribution'  : 'UniformAdaptive',
                #'initial_weight_scale'         : 0.01,
                'epochs'                       : 1.0,
                'destination_key'              : model_key,
                'validation'                   : hex_key,
            }

            timeoutSecs = 60
            start = time.time()
            h2o.beta_features = True
            h2o_cmd.runNNet(parseResult=parseResult, timeoutSecs=timeoutSecs, **kwargs)
            print "trial #", trial, "NN end on ", csvFilename, ' took', time.time() - start, 'seconds'

            #### Now score using the model, and check the validation error
            expectedErr = 0.0
            relTol = 0.01
            kwargs = {
                'source' : hex_key,
                'max_rows': 0,
                'response': 'C' + str(response),
                'ignored_cols': None, # this is not consistent with ignored_cols_by_name
                'classification': 1,
                'destination_key': 'score' + str(trial) + '.hex',
                'model': model_key
            }

            nnScoreResult = h2o_cmd.runNNetScore(key=parseResult['destination_key'], timeoutSecs=timeoutSecs, **kwargs)
            h2o_nn.checkScoreResult(self, nnScoreResult, expectedErr, relTol, **kwargs)

            h2o.check_sandbox_for_errors()

            trial += 1

if __name__ == '__main__':
    h2o.unit_main()
