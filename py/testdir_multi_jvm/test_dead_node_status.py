import unittest, time, sys
sys.path.extend(['.','..','py'])
import h2o_browse as h2b
import h2o, h2o_hosts, h2o_util, h2o_log

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(3)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        # time.sleep(3600)
        h2o.tear_down_cloud()

    def test_dead_node_status(self):
        # view logs using each node
        h2b.browseTheCloud()

        for h in h2o.nodes:
            h.log_view()

        # terminate node 1
        h2o.nodes[1].terminate_self_only()
        nodeList = h2o.nodes[:] # copy
        del nodeList[1] # 1 is dead now
        print "We probably need some status to interrogate to understand a node is in red state?"
        print "And I probably need to wait 60 secs to get to red state"
        time.sleep(5)
        h2o.verify_cloud_size(nodeList, verbose=True)
        time.sleep(5)
        h2o.verify_cloud_size(nodeList, verbose=True)
        time.sleep(5)
        h2o.verify_cloud_size(nodeList, verbose=True)

if __name__ == '__main__':
    h2o.unit_main()
