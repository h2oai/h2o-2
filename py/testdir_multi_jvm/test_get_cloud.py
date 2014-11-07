import unittest, time, sys, json, re
sys.path.extend(['.','..','../..','py'])
import h2o

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.init(3)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud(sandboxIgnoreErrors=True)

    def test_get_cloud(self):
        # Ask each node for jstack statistics. do it 100 times
        SLEEP_AFTER = False
        GET_CLOUD_ALL_NODES = True
        TRIALMAX = 25
        NODE = 1
        PRINT_GET_CLOUD = True
        eList = []
        xList = []
        sList = []
        for trial in range(TRIALMAX):
            print "Starting Trial", trial
            print "Just doing node[%s]" % NODE
            getCloudFirst = None
            for i,n in enumerate(h2o.nodes):
                if GET_CLOUD_ALL_NODES or i==NODE: # just track times on 0
                    # we just want the string
                    start = time.time()
                    getCloud = n.get_cloud()
                    elapsed = int(1000 * (time.time() - start)) # milliseconds
                    print "get_cloud completes to node", i, "in", "%s"  % elapsed, "millisecs"
                    getCloudString = json.dumps(getCloud)

                    if PRINT_GET_CLOUD:
                        print h2o.dump_json(getCloud)
                
                    h2o.verboseprint(json.dumps(getCloud,indent=2))

                    if i==NODE: # just track times on 0
                        sList.append(len(getCloudString))
                        xList.append(trial)
                        eList.append(elapsed)

                    if SLEEP_AFTER:
                        delay = 1
                        print "Sleeping for", delay, "sec"
                        time.sleep(delay)

        if h2o.python_username=='kevin':
            import pylab as plt
            if eList:
                print "xList", xList
                print "eList", eList
                print "sList", sList

                plt.figure()
                plt.plot (xList, eList)
                plt.xlabel('trial')
                plt.ylabel('get_cloud completion latency (millisecs)')
                plt.title('Back to Back get_cloud requests to node['+str(NODE)+']')
                plt.draw()

                plt.figure()
                plt.plot (xList, sList)
                plt.xlabel('trial')
                plt.ylabel('node['+str(NODE)+'] get_cloud response string length')
                plt.title('Back to Back get_cloud requests to node['+str(NODE)+']')
                plt.title('Back to Back get_cloud')
                plt.draw()

                plt.show()
        

if __name__ == '__main__':
    h2o.unit_main()
