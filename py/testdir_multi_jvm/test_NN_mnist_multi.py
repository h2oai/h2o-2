import unittest, time, sys, random, string
sys.path.extend(['.','..','py'])
import h2o, h2o_nn, h2o_cmd, h2o_hosts, h2o_import as h2i, h2o_jobs, h2o_browse as h2b

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED, localhost, tryHeap
        tryHeap = 4
        SEED = h2o.setup_random_seed()
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(3, enable_benchmark_log=True, java_heap_GB=tryHeap)
        else:
            h2o_hosts.build_cloud_with_hosts(enable_benchmark_log=True)

    @classmethod
    def tearDownClass(cls):
        ###h2o.sleep(3600)
        h2o.tear_down_cloud()

    def test_NN_mnist_multi(self):
        #h2b.browseTheCloud()
        h2o.beta_features = True
        csvPathname_train = 'mnist/train.csv.gz'
        csvPathname_test  = 'mnist/test.csv.gz'
        hex_key = 'mnist_train.hex'
        validation_key = 'mnist_test.hex'
        timeoutSecs = 30
        parseResult  = h2i.import_parse(bucket='smalldata', path=csvPathname_train, schema='put', hex_key=hex_key, timeoutSecs=timeoutSecs)
        parseResultV = h2i.import_parse(bucket='smalldata', path=csvPathname_test, schema='put', hex_key=validation_key, timeoutSecs=timeoutSecs)
        inspect = h2o_cmd.runInspect(None, hex_key)
        print "\n" + csvPathname_train, \
            "    numRows:", "{:,}".format(inspect['numRows']), \
            "    numCols:", "{:,}".format(inspect['numCols'])
        response = inspect['numCols'] - 1

        modes = [
            ###'SingleThread', ### too slow (and slightly less accurate)
            'SingleNode',  ### wastes N-1 nodes, since their weight matrices are updated but never looked at...
            ###'MapReduce' ### TODO: enable, once implemented
            ]

        for mode in modes:

            #Making random id
            identifier = ''.join(random.sample(string.ascii_lowercase + string.digits, 10))
            model_key = 'nn_' + identifier + '.hex'

            kwargs = {
                'ignored_cols'                 : None,
                'response'                     : response,
                'classification'               : 1,
                'mode'                         : mode,
                'activation'                   : 'RectifierWithDropout',
                'input_dropout_ratio'          : 0.2,
                'hidden'                       : '117,131,129',
                'rate'                         : 0.005,
                'rate_annealing'               : 1e-6,
                'momentum_start'               : 0.5,
                'momentum_ramp'                : 100000,
                'momentum_stable'              : 0.9,
                'l1'                           : 0.00001,
                'l2'                           : 0.0000001,
                'seed'                         : 98037452452,
                'loss'                         : 'CrossEntropy',
                'max_w2'                       : 15,
                'warmup_samples'               : 0,
                'initial_weight_distribution'  : 'UniformAdaptive',
                #'initial_weight_scale'         : 0.01,
                'epochs'                       : 20.0,
                'destination_key'              : model_key,
                'validation'                   : validation_key,
            }
            ###expectedErr = 0.0362 ## from single-threaded mode
            expectedErr = 0.03 ## observed actual value with Hogwild

            timeoutSecs = 600
            start = time.time()
            nn = h2o_cmd.runNNet(parseResult=parseResult, timeoutSecs=timeoutSecs, **kwargs)
            print "neural net end on ", csvPathname_train, " and ", csvPathname_test, 'took', time.time() - start, 'seconds'

            relTol = 0.02 if mode == 'SingleThread' else 0.10 ### 10% relative error is acceptable for Hogwild
            h2o_nn.checkLastValidationError(self, nn['neuralnet_model'], inspect['numRows'], expectedErr, relTol, **kwargs)

            ### Now score using the model, and check the validation error
            kwargs = {
                'source' : validation_key,
                'max_rows': 0,
                'response': response,
                'ignored_cols': None, # this is not consistent with ignored_cols_by_name
                'classification': 1,
                'destination_key': 'score_' + identifier + '.hex',
                'model': model_key,
            }
            nnScoreResult = h2o_cmd.runNNetScore(key=parseResult['destination_key'], timeoutSecs=timeoutSecs, **kwargs)
            h2o_nn.checkScoreResult(self, nnScoreResult, expectedErr, relTol, **kwargs)

            if mode != 'MapReduce':
                print 'WARNING: Running in non-MapReduce mode on multiple nodes! Only one node contributes to results.'

        h2o.beta_features = False

if __name__ == '__main__':
    h2o.unit_main()
