import unittest, random, sys
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_util, h2o_hosts, h2o_import as h2i

DO_DOWNLOAD = False
DO_INSPECT = False

paramDict = {
    'rows': [1,100,1000],
    'cols': [1,10,100], # Number of data columns (in addition to the first response column)
    'seed': [None, 1234],
    'randomize': [None, 0, 1],
    'value': [None, 0, 1234567890, 1e6, -1e6], # Constant value (for randomize=false)
    'real_range': [None, 0, 1234567890, 1e6, -1e6], # -range to range
    'categorical_fraction': [None, 0.1, 1.0], # Fraction of integer columns (for randomize=true)
    'factors': [None, 0, 1], # Factor levels for categorical variables
    'integer_fraction': [None, 0.1, 1.0], # Fraction of integer columns (for randomize=true)
    'integer_range': [None, 0, 1, 1234567890], # -range to range
    'missing_fraction': [None, 0.1, 1.0],
    'response_factors': [None, 0, 1, 2, 10], # Number of factor levels of the first column (1=real, 2=binomial, N=multinomial)
}

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED, localhost
        SEED = h2o.setup_random_seed()
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(node_count=1, base_port=54321)
        else:
            h2o_hosts.build_cloud_with_hosts()
        global SYNDATASETS_DIR
        SYNDATASETS_DIR = h2o.make_syn_dir()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_create_frame_rand1(self):
        h2o.beta_features = True
        # default
        params = {
            'rows': 1, 
            'cols': 1
        }
        for trial in range(10):
            h2o_util.pickRandParams(paramDict, params)
            i = params.get('integer_fraction', 0)
            c = params.get('categorical_fraction', 0)
            r = params.get('randomize', 0)
            v = params.get('value', None)
            if r:
                params['value'] = None
                if (i and c) and (i + c) >= 1.0:
                    params['integer_fraction'] = i
                    params['categorical_fraction'] = 1.0 - i
            else:
                params['integer_fraction'] = 0
                params['categorical_fraction'] = 0


            kwargs = params.copy()

            print kwargs
            timeoutSecs = 300
            parseResult = h2i.import_parse(bucket='smalldata', path='poker/poker1000', hex_key='temp1000.hex', 
                schema='put', timeoutSecs=timeoutSecs)
            cfResult = h2o.nodes[0].create_frame(key='temp1000.hex', timeoutSecs=timeoutSecs, **kwargs)

            if DO_DOWNLOAD:
                csvPathname = SYNDATASETS_DIR + '/' + 'temp1000.csv'
                h2o.nodes[0].csv_download(src_key='temp1000.hex', csvPathname=csvPathname, timeoutSecs=60)

            if DO_INSPECT:
                h2o_cmd.runInspect(key='temp1000.hex')

            h2o_cmd.runSummary(key='temp1000.hex')
            print h2o.dump_json(cfResult)
    
            print "Trial #", trial, "completed"

if __name__ == '__main__':
    h2o.unit_main()
