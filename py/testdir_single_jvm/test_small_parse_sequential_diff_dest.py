import sys, unittest, random, time
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts, h2o_import2 as h2i

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global localhost
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(1)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod 
    def tearDownClass(cls): 
        h2o.tear_down_cloud()
 
    def test_small_parse_sequential_diff_dest(self):
        csvPathname = 'poker/poker-hand-testing.data'
        for trials in range(100):
            hex_key = csvPathname + "_" + str(trials)
            parseResult = h2i.import_parse(bucket='smalldata', path=csvPathname, schema='put',
                hex_key=hex_key, timeoutSecs=120, noPoll=False, doSummary=False)

if __name__ == "__main__":
	h2o.unit_main()
