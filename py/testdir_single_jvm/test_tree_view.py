import unittest, sys
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_hosts, h2o_import as h2i

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

    def test_tree_view(self):
        parseResult = h2i.import_parse(bucket='smalldata', path='poker/poker1000', hex_key='poker1000.hex', schema='put')
        h2o_cmd.runRF(parseResult=parseResult, trees=50, model_key="model0", timeoutSecs=10)

        for n in range(1):
            a = h2o_cmd.runRFTreeView(n=n, data_key='poker1000.hex', model_key="model0", timeoutSecs=10)
            print (h2o.dump_json(a))

if __name__ == '__main__':
    h2o.unit_main()
