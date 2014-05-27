import unittest, time, sys
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd,h2o_hosts, h2o_browse as h2b, h2o_import as h2i, h2o_hosts
import time, random

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global localhost
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(1,java_heap_GB=10)
        else:
            h2o_hosts.build_cloud_with_hosts(1,java_heap_GB=10)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_from_import(self):
        timeoutSecs = 500
        csvFilenameAll = [
            "covtype.data",
            "covtype20x.data",
            ]

        # pop open a browser on the cloud
        # h2b.browseTheCloud()

        for csvFilename in csvFilenameAll:
            # creates csvFilename.hex from file in importFolder dir 
            hex_key = csvFilename + '.hex'
            parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path="standard/" + csvFilename, schema='put',
                hex_key=hex_key, timeoutSecs=500)
            if not h2o.beta_features:
                print csvFilename, 'parse time:', parseResult['response']['time']
            print "Parse result['destination_key']:", parseResult['destination_key']
            inspect = h2o_cmd.runInspect(key=parseResult['destination_key'])

            if not h2o.beta_features:
                RFview = h2o_cmd.runRF(trees=1,depth=25,parseResult=parseResult, timeoutSecs=timeoutSecs)

            ## h2b.browseJsonHistoryAsUrlLastMatch("RFView")
            ## time.sleep(10)

            # just to make sure we test this
            h2i.delete_keys_at_all_nodes(pattern=hex_key)

if __name__ == '__main__':
    h2o.unit_main()
