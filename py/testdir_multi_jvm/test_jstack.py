import unittest, time, sys, json, re
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
        SLEEP_AFTER = False
        JSTACK_ALL_NODES = False
        TRIALMAX = 25
        NODE = 1
        PRINT_JSTACK = False
        eList = []
        xList = []
        sList = []
        for trial in range(TRIALMAX):
            print "Starting Trial", trial
            print "Just doing node[%s]" % NODE
            statsFirst = None
            for i,n in enumerate(h2o.nodes):
                if JSTACK_ALL_NODES or i==NODE: # just track times on 0
                    # we just want the string
                    start = time.time()
                    stats = n.jstack()
                    elapsed = int(1000 * (time.time() - start)) # milliseconds
                    print "Jstack completes to node", i, "in", "%s"  % elapsed, "millisecs"
                    statsString = json.dumps(stats)

                    if PRINT_JSTACK:
                        # statsString = re.sub(r'at \\n','at ',statsString)
                        # statsString = re.sub(r'\\n','\n',statsString)
                        statsString = re.sub(r'\\n','\n',statsString)
                        statsString = re.sub(r'\\t','\t',statsString)
                        # statsString = re.sub(r'\\t','\t',statsString)
                        
                        procs = statsString.split(' ')
                        # look for interesting ones
                        i = 0
                        while i < len(procs):
                            if 'JStack' in procs[i]:
                                # print the next 10
                                print "\n" + procs[i]
                                for j in range(10):
                                    print procs[i+j]
                                i = i + 10
                            else:
                                i = i + 1
                
                    h2o.verboseprint(json.dumps(stats,indent=2))

                    if i==NODE: # just track times on 0
                        sList.append(len(statsString))
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
