import unittest, random, sys, time, json
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_kmeans, h2o_import as h2i, h2o_util

def define_create_frame_params(SEED):
    paramDict = {
        # minimum of 5 rows to cover the 5 cluster case
        'rows': [5, 100, 1000],
        'cols': [10, 100], # Number of data columns (in addition to the first response column)
        'seed': [None, 1234],
        'randomize': [None, 1], # Avoid all constant (randomize=0) -> all points would be the same (except for NAs) -> can't find K clusters
        'value': [None, 0, 1234567890, 1e6, -1e6], # Constant value (for randomize=false)
        'real_range': [None, 0, 1234567890, 1e6, -1e6], # -range to range
        'categorical_fraction': [None, 0.1, 1.0], # Fraction of integer columns (for randomize=true)
        'binary_fraction': [0], # Fraction of binary columns (for randomize=true)
        'factors': [None, 2, 10], # Factor levels for categorical variables
        'integer_fraction': [None, 0.1, 1.0], # Fraction of integer columns (for randomize=true)
        'integer_range': [None, 0, 1, 1234567890], # -range to range
        'missing_fraction': [None, 0.1],
        'response_factors': [None, 1, 2, 10], # Number of factor levels of the first column (1=real, 2=binomial, N=multinomial)
    }
    return paramDict


def define_KMeans_params(SEED):
    paramDict = {
        'k': [2, 5], # seems two slow tih 12 clusters if all cols
        'initialization': ['None', 'PlusPlus', 'Furthest'],
        'ignored_cols': [None, "0", "3", "0,2"],
        'seed': [None, 12345678, SEED],
        'normalize': [None, 0, 1],
        'max_iter': [1,14],
        # 'destination_key:': "junk",
        
        }
    return paramDict

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = h2o.setup_random_seed()
        h2o.init(3,java_heap_GB=4)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_KMeans_create_frame_fvec(self):
        for trial in range(20):

            cfParamDict = define_create_frame_params(SEED)
            # default
            params = {
                'rows': 5,
                'cols': 10
            }
            h2o_util.pickRandParams(cfParamDict, params)
            b = params.get('binary_fraction', None)
            i = params.get('integer_fraction', None)
            c = params.get('categorical_fraction', None)
            r = params.get('randomize', None)
            v = params.get('value', None)

            # h2o does some strict checking on the combinations of these things
            # fractions have to add up to <= 1 and only be used if randomize
            # h2o default randomize=1?
            if not b:
                b = 0
            if not i:
                i = 0
            if not c:
                c = 0

            # force a good combo, by decreasing t2o a little at a time
            while (i + b + c) > 1.0:
                print "Trying to find a good mix of fractional", b, i, c
                b = max(0, b - 0.13)
                i = max(0, i - 0.17)
                # what's left
                c = 1.0 - (i + b)

            params['binary_fraction'] = b
            params['integer_fraction'] = i
            params['categorical_fraction'] = c
            params['value'] = None


            kwargs = params.copy()
            timeoutSecs = 300
            hex_key = 'temp_%s.hex' % trial
            cfResult = h2o.nodes[0].create_frame(key=hex_key, timeoutSecs=timeoutSecs, **kwargs)
            inspect = h2o_cmd.runInspect(None, hex_key)
            print "\n%s" % hex_key, \
                "    numRows:", "{:,}".format(inspect['numRows']), \
                "    numCols:", "{:,}".format(inspect['numCols'])

            kmeansParamDict = define_KMeans_params(SEED)

            # default
            params = {
                'max_iter': 20, 
                'k': 1, 
                'destination_key': "KM_" + str(trial) + '.hex'
            }
            h2o_kmeans.pickRandKMeansParams(kmeansParamDict, params)
            kwargs = params.copy()

            start = time.time()
            parseResult = {'destination_key': hex_key }
            kmeans = h2o_cmd.runKMeans(parseResult=parseResult, \
                timeoutSecs=timeoutSecs, retryDelaySecs=2, pollTimeoutSecs=60, **kwargs)
            elapsed = time.time() - start
            print "kmeans trial %s end on ", trial, 'took', elapsed, 'seconds.', \
                "%d pct. of timeout" % ((elapsed/timeoutSecs) * 100)
            h2o_kmeans.simpleCheckKMeans(self, kmeans, **kwargs)

            ### print h2o.dump_json(kmeans)

            print "Trial #", trial, "completed\n"

if __name__ == '__main__':
    h2o.unit_main()
