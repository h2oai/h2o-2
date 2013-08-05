import unittest, time, sys
sys.path.extend(['.','..','py'])
import h2o, h2o_hosts, h2o_cmd

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

    def test_put_parse4(self):
        timeoutSecs = 10
        trial = 1
        n = h2o.nodes[0]
        for x in xrange (2):
            print 'Trial:', trial
            # csvPathname = h2o.find_file("smalldata/hhp_107_01.data.gz")
            csvPathname = h2o.find_file('smalldata/iris/iris_wheader.csv.gz')
            key2 = "iris" + "_" + str(x) + ".hex"
            parseKey = h2o_cmd.parseFile(csvPathname=csvPathname, key2=key2, doSummary=False)
            h2o_cmd.runSummary(key=key2, doPrint=True)
            trial += 1


if __name__ == '__main__':
    h2o.unit_main()
