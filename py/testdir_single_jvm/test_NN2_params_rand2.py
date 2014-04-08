import unittest, random, sys, time
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts, h2o_glm, h2o_import as h2i, h2o_nn

def define_params(): 
    paramDict = {
        'destination_key'              : [None, 'NN2_model'],
        'ignored_cols'                 : [None, 0, 1],
        'classification'               : [None, 0, 1],
        'validation'                   : [None, 'covtype.20k.hex'],
        # 'mode'                         : [None, 'SingleNode', 'SingleThread', 'MapReduce'], 
        'activation'                   : [None, 'Tanh', 'TanhWithDropout', 'Rectifier', 'RectifierWithDropout', 
                                            'Maxout', 'MaxoutWithDropout'],
        'input_dropout_ratio'          : [None, 0, 1],
        'hidden'                       : [None, 1, '200,200'],
        'rate'                         : [None, 0, 0.005, 0.010],
        'rate_annealing'               : [None, 0, 1e-6, 1e-4],
        'momentum_start'               : [None, 0, 0.1, 0.5, 0.9999],
        'momentum_ramp'                : [None, 0, 10000, 1000000],
        'momentum_stable'              : [None, 0, 0.9, 0.8],
        'max_w2'                       : [None, 0, 5, 10, 'Infinity'],
        'l1'                           : [None, 0, 1e-4],
        'l2'                           : [None, 0, 1e-4, 0.5],
        'seed'                         : [None, 0, 1, 5234234],
        'initial_weight_distribution'  : [None, 'UniformAdaptive', 'Uniform', 'Normal'],
        'initial_weight_scale'         : [None, 0, 1],
        'loss'                         : [None, 'MeanSquare', 'CrossEntropy'],
        'rate_decay'                   : [None, 0, 1],
        'epochs'                       : [None, 0, 1],
        'score_training_samples'       : [None, 0, 1],
        'score_validation_samples'     : [None, 0, 1],
        'score_interval'               : [None, 0, 1],
        'train_samples_per_iteration'  : [None, 0, 1],
        'diagnostics'                  : [None, 0, 0, 0, 0, 1],
        'fast_mode'                    : [None, 0, 1],
        'ignore_const_cols'            : [None, 0, 1],
        'shuffle_training_data'        : [None, 0, 1],
        'nesterov_accelerated_gradient': [None, 0, 1],
        # 'warmup_samples'               : [None, 0, 10],
    }

    return paramDict
    
class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED, localhost
        SEED = h2o.setup_random_seed()
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(node_count=1)
        else:
            h2o_hosts.build_cloud_with_hosts(node_count=1)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_NN2_params_rand2(self):
        csvPathname = 'covtype/covtype.20k.data'
        hex_key = 'covtype.20k.hex'
        parseResult = h2i.import_parse(bucket='smalldata', path=csvPathname, hex_key=hex_key, schema='put')
        paramDict = define_params()

        for trial in range(5):
            # params is mutable. This is default.
            params = {'response': 'C55'}
            h2o_nn.pickRandDeepLearningParams(paramDict, params)
            kwargs = params.copy()
            start = time.time()
            nn = h2o_cmd.runDeepLearning(timeoutSecs=300, parseResult=parseResult, **kwargs)
            print "nn result:", h2o.dump_json(nn)
            h2o.check_sandbox_for_errors()
            # FIX! simple check?

            print "Deep Learning end on ", csvPathname, 'took', time.time() - start, 'seconds'
            print "Trial #", trial, "completed\n"

if __name__ == '__main__':
    h2o.unit_main()
