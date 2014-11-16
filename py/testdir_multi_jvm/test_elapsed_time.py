import unittest, time, sys
sys.path.extend(['.','..','../..','py'])
import h2o_browse as h2b
import h2o, h2o_util, h2o_log


NODE_NUM = 3
class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.init(NODE_NUM)

    @classmethod
    def tearDownClass(cls):
        # time.sleep(3600)
        h2o.tear_down_cloud()

    def test_elapsed_time(self):
        h2b.browseTheCloud()

        print "The reported time should increment for each node, on every node."

        for n in range(NODE_NUM):
            c = h2o.nodes[n].get_cloud()
            self.assertEqual(c['cloud_healthy'], True)
            # the node order doesn't match our node order
            
        # start with elapsed_time history = 0
        etime = [ 0 for i in range(NODE_NUM)]

        # loop checking delapsed time increments
        def check_and_update_etime():
            for n in range(NODE_NUM):
                c = h2o.nodes[n].get_cloud()
                for i in range(NODE_NUM):
                    t = c['nodes'][i]['elapsed_time']
                    n = c['nodes'][i]['name']
                    h = c['nodes'][i]['node_healthy']
                    print "Current elapsed_time: %s for %s" % (t, n)
                    if t < etime[i]:
                        msg="Current elapsed_time: %s at %s is not > its last polled elapsed_time %s" % (t, n, etime[i])

                    etime[i] = t
                    self.assertEqual(h, True)

        for j in range(10):
            time.sleep(2)
            check_and_update_etime()
    

if __name__ == '__main__':
    h2o.unit_main()
