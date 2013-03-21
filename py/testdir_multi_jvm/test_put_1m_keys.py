import unittest, sys, random
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_browse as h2b, h2o_hosts

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
        h2o.tear_down_cloud()

    def test_put_1m_keys(self):
        lenNodes = len(h2o.nodes)
        h2b.browseTheCloud()

        trial = 0
        for i in range(5):
            for j in range(1,10000):
                # bounce it around the nodes in the cloud
                nodeX = random.randint(0,lenNodes-1) 
                # print nodeX 
                # FIX! can add repl
                p =  h2o.nodes[nodeX].put_value(trial, key=trial)
                trial += 1

            # dump some cloud info so we can see keys?
            print "\nAt trial #" + str(trial)

            c = h2o.nodes[0].get_cloud()
            print (h2o.dump_json(c))

if __name__ == '__main__':
    h2o.unit_main()
