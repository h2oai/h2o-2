import unittest, sys, time
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i

IGNORE_ERRORS = False
class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.init(2)
        ### h2b.browseTheCloud()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud(sandboxIgnoreErrors=IGNORE_ERRORS)

    def test_network(self):
        for i in range(5):
            for node_index, node in enumerate(h2o.nodes):
                result = node.network_test()
                h2o.check_sandbox_for_errors(sandboxIgnoreErrors=IGNORE_ERRORS)
                print "network test by node %s:" % node_index, h2o.dump_json(result)
                bws = result['bandwidths']
                
                # break out the bws for each msg size into separate lists per node
                # then check that they are bigger as the msg size increases

                # ignore 0, and then check that the others are close to each other?
                
                per_node_bws = [[] for i in range(len(h2o.nodes))]
                for b,bw in enumerate(bws):
                    for n,node_bw in enumerate(bw):
                        if node_bw < 1.0 or node_bw > 1e12:
                            raise Exception("Node bw seems odd: %s %s %s" % (node_index, n, node_bw))
                        per_node_bws[n].append(node_bw)
                

                for p, pnb in enumerate(per_node_bws):
                    # sort them. the original and sorted should both be ascending bws
                    if pnb!=sorted(pnb):
                        raise Exception("running at node %s, bws didn't increase with msg size for node %s: %s" (n, p, pnb))

                # if there is more than one node, check that each msg size bw value is within 20% of the first value (node 1)
                # hmm..does arno consider node 0 the same as I do?
    

        

        # print the biggest msg bws the last node tested
        for p, pnb in enumerate(per_node_bws):
            print "arno node id: %s" % p, "largest msg, %e bytes/sec" % pnb[-1]
        

        # get assertion failure on shutdown? wait a bit
        time.sleep(5)

if __name__ == '__main__':
    h2o.unit_main()
