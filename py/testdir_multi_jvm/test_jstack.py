import unittest, time, sys, json
sys.path.extend(['.','..','py'])
import h2o, h2o_hosts


class JStackApi(unittest.TestCase):
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
        h2o.tear_down_cloud(sandboxIgnoreErrors=True)

    def test_jstack(self):
        # Ask each node for jstack statistics. do it 100 times
        TRIALMAX = 25
        NODE = 1
        eList = []
        xList = []
        sList = []
        for trial in range(TRIALMAX):
            print "Starting Trial", trial
            print "Just doing node[%s]" % NODE
            for i,n in enumerate(h2o.nodes):
                if i==NODE: # just track times on 0
                    start = time.time()
                    # we just want the string
                    stats = json.dumps(n.jstack())
                    elapsed = int( 1000 * (time.time() - start)) # milliseconds

                    sList.append(len(stats))
                    xList.append(trial)
                    eList.append(elapsed)

                    h2o.verboseprint(json.dumps(stats,indent=2))
                    print "Jstack completes to node", i, "in", "%s"  % elapsed, "millisecs"
                print "Sleeping for 1 sec"
                time.sleep(1)

        if h2o.python_username=='kevin':
            import pylab as plt
            if eList:
                print "xList", xList
                print "eList", eList
                print "sList", sList

                plt.figure()
                plt.plot (xList, eList)
                plt.xlabel('trial')
                plt.ylabel('Jstack completion latency (millisecs)')
                plt.title('Back to Back Jstack requests to node['+str(NODE)+']')
                plt.draw()

                plt.figure()
                plt.plot (xList, sList)
                plt.xlabel('trial')
                plt.ylabel('node['+str(NODE)+'] Jstack response string length')
                plt.title('Back to Back Jstack requests to node['+str(NODE)+']')
                plt.title('Back to Back Jstack')
                plt.draw()

                plt.show()
        

if __name__ == '__main__':
    h2o.unit_main()
