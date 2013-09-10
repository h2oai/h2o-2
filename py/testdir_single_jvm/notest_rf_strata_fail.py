import unittest, random, sys
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_rf, h2o_hosts, h2o_import2 as h2i

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global localhost
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(node_count=1)
        else:
            h2o_hosts.build_cloud_with_hosts(node_count=1)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_rf_strata_fail(self):
        csvPathname ='UCI/UCI-large/covtype/covtype.data'
        timeoutSecs = 60
        kwargs = {
            'response_variable': 54,
            'ntree': 50,
            'features': '',
            'depth': 2147483647,
            'stat_type': 'ENTROPY',
            'ignore': '',
            'class_weights': '1=1.0,2=1.0,3=1.0,4=1.0,5=1.0,6=1.0,7=1.0',
            'sampling_strategy': 'RANDOM',
            'strata_samples': 'undefined=undefined,undefined=undefined,undefined=undefined,undefined=undefined,undefined=undefined,undefined=undefined,undefined=undefined',
            'sample': '67',
            'out_of_bag_error_estimate': 1,
            'model_key': '',
            'bin_limit': 1024,
            'seed': 784834182943470027,
            'parallel': 1,
            'exclusive_split_limit': '', 
            'iterative_cm': 1,
            'use_non_local_data': 0,
        }
        parseResult = h2i.import_parse(bucket='datasets', path=csvPathname, schema='put')
        h2o_cmd.runRF(parseResult=parseResult, timeoutSecs=timeoutSecs, **kwargs)

if __name__ == '__main__':
    h2o.unit_main()
