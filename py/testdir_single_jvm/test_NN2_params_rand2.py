import unittest, random, sys, time
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_glm, h2o_import as h2i, h2o_nn

def define_params(): 
    paramDict = {
        'destination_key'              : [None, 'NN2_model'],
        'ignored_cols'                 : [None, 0, 1, '0,1'],
        'classification'               : [None, 0, 1],
        'validation'                   : [None, 'covtype.20k.hex'],
        # 'mode'                         : [None, 'SingleNode', 'SingleThread', 'MapReduce'], 
        'activation'                   : [None, 'Tanh', 'TanhWithDropout', 'Rectifier', 'RectifierWithDropout', 
                                            'Maxout', 'MaxoutWithDropout'],
        'input_dropout_ratio'          : [None, 0, 0.5, .99], # 1 is illegal
        'hidden'                       : [None, 1, '100,50'],
        'adaptive_rate'                : [None, 0, 1],
        'rate'                         : [None, 0.005, 0.010],
        'rate_annealing'               : [None, 0, 1e-6, 1e-4],
        'momentum_start'               : [None, 0, 0.1, 0.5, 0.9999],
        'momentum_ramp'                : [None, 1, 10000, 1000000],
        'momentum_stable'              : [None, 0, 0.9, 0.8],
        'max_w2'                       : [None, 5, 10, 'Infinity'],
        'l1'                           : [None, 0, 1e-4],
        'l2'                           : [None, 0, 1e-4, 0.5],
        'seed'                         : [None, 0, 1, 5234234],
        'initial_weight_distribution'  : [None, 'UniformAdaptive', 'Uniform', 'Normal'],
        'initial_weight_scale'         : [None, 0, 1],
        'rate_decay'                   : [None, 0, 1],
        'epochs'                       : [0.001, 2],
        'score_training_samples'       : [None, 0, 1],
        'score_validation_samples'     : [None, 0, 1],
        'score_interval'               : [None, 0, 1],
        'train_samples_per_iteration'  : [None, 0, 1],
        'diagnostics'                  : [None, 0, 1],
        'force_load_balance'           : [None, 0, 1],
        'replicate_training_data'      : [None, 0, 1],
        'shuffle_training_data'        : [None, 0, 1],
        'score_duty_cycle'             : [None, 0.1, 0.01],
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
        global SEED
        SEED = h2o.setup_random_seed()
        h2o.init()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_NN2_params_rand2(self):
        csvPathname = 'covtype/covtype.20k.data'
        hex_key = 'covtype.20k.hex'
        parseResult = h2i.import_parse(bucket='smalldata', path=csvPathname, hex_key=hex_key, schema='put')
        paramDict = define_params()

        for trial in range(3):
            # params is mutable. This is default.
            params = {'response': 'C55', 'epochs': '1'}
            h2o_nn.pickRandDeepLearningParams(paramDict, params)
            kwargs = params.copy()
            start = time.time()
            nn = h2o_cmd.runDeepLearning(timeoutSecs=500, parseResult=parseResult, **kwargs)
            print "nn result:", h2o.dump_json(nn)
            h2o.check_sandbox_for_errors()


            deeplearning_model = nn['deeplearning_model']
            errors = deeplearning_model['errors']
            # print "errors", h2o.dump_json(errors)
            # print "errors, classification", errors['classification']

            # assert 1==0
            # unstable = nn['model_info']['unstable']

            # unstable case caused by : 
            # normal initial distribution with amplitude 1 and input_dropout_ratio=1.  
            # blowing up numerically during propagation of all zeroes as input repeatedly.  
            # arnon added logging to stdout in addition to html in 7899b92ad67.  
            # Will have to check that first before making predictions.

            # print "unstable:", unstable

            # FIX! simple check?

            print "Deep Learning end on ", csvPathname, 'took', time.time() - start, 'seconds'
            print "Trial #", trial, "completed\n"

if __name__ == '__main__':
    h2o.unit_main()
