import sys, unittest, random, time
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts


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
 
    def test_big_parse_overlap_same_dest_del_nopoll(self):
		noPoll = True
		timeoutSecs = 180
		num_trials = 0
		trial_max = 100
		while num_trials < trial_max:
			num_trials += 1
			csvPathname = h2o.find_file('smalldata/mnist')
			csvFilename = csvPathname + '/' + 'mnist8m-test-1.csv'
			key = csvFilename
			key2 = csvFilename + '.hex'
			parseKey = h2o_cmd.parseFile(csvPathname=csvFilename, 
    			key=key, key2=key2, timeoutSecs=timeoutSecs, noPoll=noPoll,
    			doSummary=False)
			node = h2o.nodes[0]
			node.remove_key(key)
			node.remove_key(key2)

if __name__ == "__main__":
	h2o.unit_main()
