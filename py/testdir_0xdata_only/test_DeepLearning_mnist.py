import unittest, time, sys, random, string

sys.path.extend(['.','..','../..','py'])
import h2o, h2o_gbm, h2o_cmd, h2o_import as h2i, h2o_browse as h2b

RANDOM_UDP_DROP = False
class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        # want detail on the cloud building to see what node fails
        h2o.verbose = True
        h2o.init(java_heap_GB=2, random_udp_drop=RANDOM_UDP_DROP)
        h2o.verbose = False

    @classmethod
    def tearDownClass(cls):
        ###h2o.sleep(3600)
        h2o.tear_down_cloud()

    def test_DeepLearning_mnist(self):
        #h2b.browseTheCloud()
        csvPathname_train = 'mnist/train.csv.gz'
        csvPathname_test  = 'mnist/test.csv.gz'
        hex_key = 'mnist_train.hex'
        validation_key = 'mnist_test.hex'
        timeoutSecs = 300
        parseResult  = h2i.import_parse(bucket='smalldata', path=csvPathname_train, schema='put', hex_key=hex_key, timeoutSecs=timeoutSecs)
        parseResultV = h2i.import_parse(bucket='smalldata', path=csvPathname_test, schema='put', hex_key=validation_key, timeoutSecs=timeoutSecs)

        inspect = h2o_cmd.runInspect(None, hex_key)
        print "\n" + csvPathname_train, \
            "    numRows:", "{:,}".format(inspect['numRows']), \
            "    numCols:", "{:,}".format(inspect['numCols'])
        response = inspect['numCols'] - 1

        #Making random id
        identifier = ''.join(random.sample(string.ascii_lowercase + string.digits, 10))
        model_key = 'deeplearning_' + identifier + '.hex'

        kwargs = {
            'ignored_cols'                 : None,
            'response'                     : response,
            'classification'               : 1,
            'activation'                   : 'RectifierWithDropout',
            'input_dropout_ratio'          : 0.2,
            'hidden'                       : '1024,1024,2048',
            'adaptive_rate'                : 1,
            'rho'                          : 0.99,
            'epsilon'                      : 1e-8,
            'train_samples_per_iteration'  : -1, ## 0: better accuracy!  -1: best scalability!  10000: best accuracy?
#            'rate'                         : 0.01,
#            'rate_annealing'               : 1e-6,
#            'momentum_start'               : 0.5,
#            'momentum_ramp'                : 1800000,
#            'momentum_stable'              : 0.99,
            'l1'                           : 1e-5,
            'l2'                           : 0.0,
            'seed'                         : 98037452452,
            'loss'                         : 'CrossEntropy',
            'max_w2'                       : 15,
            'initial_weight_distribution'  : 'UniformAdaptive',
            'epochs'                       : 128, #enough for 64 nodes
            'destination_key'              : model_key,
            'validation'                   : validation_key,
            'score_interval'               : 10000 #don't score until the end
            }

        timeoutSecs = 7200
        start = time.time()
        deeplearning = h2o_cmd.runDeepLearning(parseResult=parseResult, timeoutSecs=timeoutSecs, **kwargs)
        print "neural net end on ", csvPathname_train, " and ", csvPathname_test, 'took', time.time() - start, 'seconds'

        predict_key = 'score_' + identifier + '.hex'

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
        actualErr = h2o_gbm.pp_cm_summary(cm)/100.;

        print "actual   classification error:" + format(actualErr)


if __name__ == '__main__':
    h2o.unit_main()
