import unittest, random, sys
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_util, h2o_import as h2i

DO_DOWNLOAD = False
DO_INSPECT = False

print "Force a param set that fails with summary..see below"

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = h2o.setup_random_seed()
        h2o.init()
        global SYNDATASETS_DIR
        SYNDATASETS_DIR = h2o.make_syn_dir()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_NOPASS_create_frame_fail(self):

        for trial in range(20):
            kwargs = {'integer_range': None, 'missing_fraction': 0.1, 'cols': 10, 'response_factors': 1, 'seed': 1234, 'randomize': 1, 'categorical_fraction': 0, 'rows': 1, 'factors': 0, 'real_range': 0, 'value': None, 'integer_fraction': 0}

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

            rSummary = h2o_cmd.runSummary(key='temp1000.hex', cols=10)
            h2o_cmd.infoFromSummary(rSummary)

            print h2o.dump_json(cfResult)
    
            print "Trial #", trial, "completed"

if __name__ == '__main__':
    h2o.unit_main()
