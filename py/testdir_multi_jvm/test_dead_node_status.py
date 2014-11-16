import unittest, time, sys
sys.path.extend(['.','..','../..','py'])
import h2o_browse as h2b
import h2o, h2o_util, h2o_log

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.init(3)

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
        # remember which is [1] so we can check cloud state correctly
        badPort = "/" + str(h2o.nodes[1].http_addr) + ":" + str(h2o.nodes[1].port)

        nodeList = h2o.nodes[:] # copy
        del nodeList[1] # 1 is dead now
        print "We probably need some status to interrogate to understand a node is in red state?"
        print "And I probably need to wait 60 secs to get to red state"
        time.sleep(120)
        # h2o.verify_cloud_size(nodeList, verbose=True, ignoreHealth=True)
        # time.sleep(5)
        # h2o.verify_cloud_size(nodeList, verbose=True, ignoreHealth=True)
        # time.sleep(5)
        # h2o.verify_cloud_size(nodeList, verbose=True, ignoreHealth=True)

        # just check that node_healthy' goes 'false' on that node
        # and 'cloud_healthy' goes false
        
        # everyone should see the same stuff (0 and 2, 1 won't respond)
        for n in (0,2):
            c = h2o.nodes[n].get_cloud()
            # the node order doesn't match our node order
            for i in range(3):
                expected = c['nodes'][i]['name']!=badPort
                self.assertEqual(c['nodes'][i]['node_healthy'], expected)

            self.assertEqual(c['cloud_healthy'], False, msg="node %s shouldn't think the cloud is healthy: %s" % (n, c['cloud_healthy']))

if __name__ == '__main__':
    h2o.unit_main()
