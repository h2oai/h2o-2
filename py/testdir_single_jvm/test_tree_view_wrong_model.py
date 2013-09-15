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

    def test_tree_view_wrong_model(self):
        csvPathname = 'poker/poker1000'
        hex_key = csvPathname + ".hex"
        # tree view failed with poker1000, passed with iris
        parseResult = h2i.import_parse(bucket='smalldata', path=csvPathname, schema='put', hex_key=hex_key, timeoutSecs=10)
        h2o_cmd.runRF(parseResult=parseResult, trees=1, model_key="model0", timeoutSecs=10)

        for n in range(1):
            # Give it the wrong model_key name. This caused a stack track
            a = h2o_cmd.runRFTreeView(n=n, 
                data_key=hex_key, model_key="wrong_model_name", timeoutSecs=10, ignoreH2oError=True)
            # FIX! we could check the return message, but for now, just not crashing is good
            # already printed in check_request
            ## print (h2o.dump_json(a))

if __name__ == '__main__':
    h2o.unit_main()
