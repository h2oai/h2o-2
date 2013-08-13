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
            h2o.build_cloud(1, java_heap_GB=10)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod 
    def tearDownClass(cls): 
        h2o.tear_down_cloud()
 
    def test_big_parse_overlap_diff_dest_nopoll(self):
			noPoll = True
			timeoutSecs = 180
			num_trials = 0
			trial_max = 4
			keys = []
			key2s = []
			while num_trials < trial_max:
				csvPathname = h2o.find_file('smalldata/mnist')
				csvFilename = csvPathname + '/' + 'mnist8m-test-1.csv'
				key = csvFilename
				key2 = key + "_" + str(num_trials) + '.hex'
				keys.append(key)
				key2s.append(key2)
				parseKey = h2o_cmd.parseFile(csvPathname=csvFilename, 
    				key=key, key2=key2, timeoutSecs=timeoutSecs, noPoll=noPoll,
    				doSummary=False)
				num_trials += 1
				node=h2o.nodes[0]
				#[node.remove_key(key) for key in keys]
				#[node.remove_key(key2) for key2 in key2s]

if __name__ == "__main__":
	h2o.unit_main()
