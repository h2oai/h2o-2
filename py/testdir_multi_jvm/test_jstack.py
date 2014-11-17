import unittest, time, sys, json, re
sys.path.extend(['.','..','../..','py'])
import h2o


class JStackApi(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        # hitting some jstack exception stack traces. ignore all
        h2o.init(3, sandbox_ignore_errors=True)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud(sandboxIgnoreErrors=True)

    def test_jstack(self):
        # Ask each node for jstack statistics. do it 100 times
        SLEEP_AFTER = False
        JSTACK_ALL_NODES = True
        TRIALMAX = 25
        NODE = 1
        PRINT_JSTACK = False
        eList = []
        xList = []
        sList = []
        for trial in range(TRIALMAX):
            print "Starting Trial", trial
            if JSTACK_ALL_NODES:
                print "Sending JStack to each node[%s]" % NODE
            else:
                print "Just sending JStack to node[%s]" % NODE
            statsFirst = None
            for i,n in enumerate(h2o.nodes):
                if JSTACK_ALL_NODES or i==NODE: # just track times on 0
                    # we just want the string
                    start = time.time()
                    stats = n.jstack(timeoutSecs=30)
                    elapsed = int(1000 * (time.time() - start)) # milliseconds
                    print "Jstack completes to node", i, "in", "%s"  % elapsed, "millisecs"
                    ## print h2o.dump_json(stats)
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

        # check that you can read the logs?
        namelist = h2o.nodes[0].log_download()

        # if h2o.python_username=='kevin':
        if 1==0:
            import pylab as plt
            if eList:
                print "xList", xList
                print "eList", eList
                print "sList", sList

                label = "Repeated JStack"
                if SLEEP_AFTER:
                    label += " SLEEP_AFTER"
                if JSTACK_ALL_NODES:
                    label += " JSTACK_ALL_NODES"
                if NODE:
                    label += " just plotting for " + str(NODE)


                plt.figure()
                plt.plot (xList, eList)
                plt.xlabel('trial')
                plt.ylabel('Jstack completion latency (millisecs)')
                plt.title(label)
                plt.draw()

                plt.figure()
                plt.plot (xList, sList)
                plt.xlabel('trial')
                plt.ylabel('node['+str(NODE)+'] Jstack response string length')
                plt.title(label)
                plt.draw()

                plt.show()
        

if __name__ == '__main__':
    h2o.unit_main()
