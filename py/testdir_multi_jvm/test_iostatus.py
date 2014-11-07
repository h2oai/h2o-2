import unittest, time, sys
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
        h2o.tear_down_cloud()

    def test_iostatus(self):
        # wait a bit first?
        time.sleep(5)
        # Ask each node for iostatus statistics
        for node in h2o.nodes:
            stats = node.iostatus()
            h2o.verboseprint(h2o.dump_json(stats))
            histogram = stats['histogram'] 
# {
# u'i_o': u'TCP', 
# u'peak_bytes_/_sec': 199690496.78920883, 
# u'effective_bytes_/_sec': 21850666.666666668, 
# u'r_w': u'write', 
# u'cloud_node_idx': 2, 
# u'window': 10
# }
            print "\nProbing node:", str(node.h2o_addr) + ":" + str(node.port)
            for k in histogram:
                ### print k
                if k['window'] == 10:
                    i_o = k['i_o']
                    node = k['cloud_node_idx']
                    r_w = k['r_w']

                    for l,v in k.iteritems():
                        fmt = "iostats: window10 node {:d} {:s} {:s} {:s} MB/sec: {:.2f}" 
                        if 'peak' in l:
                            print fmt.format(node, i_o, r_w, "peak", (v/1e6))
                        if 'effective' in l:
                            print fmt.format(node, i_o, r_w, "eff.", (v/1e6))

# {
# u'node': u'/192.168.0.37:54321', 
# u'i_o': u'TCP', 
# u'closeTime': '10:31:47:370', 
# u'r_w': u'write', 
# u'duration_ms': 4, 
# u'blocked_ns': 463132, 
# u'size_bytes': 65552
# }
            # we want to sort the results before we print them, so grouped by node
            iopList = []
            raw_iops = stats['raw_iops'] 
            print
            for k in raw_iops:
                ### print k
                node = k['node']
                i_o = k['i_o']
                r_w = k['r_w']
                size = k['size_bytes']
                blocked = k['blocked_ms']
                duration = k['duration_ms'] * 1e6 # convert to ns
                if duration != 0:
                    blockedPct = "%.2f" % (100 * blocked/duration) + "%"
                else:
                    blockedPct = "no duration"
                iopMsg = "node: %s %s %s %d bytes. blocked: %s" % (node, i_o, r_w, size, blockedPct)
                iopList.append([node, iopMsg])

            iopList.sort(key=lambda iop: iop[0])  # sort by node
            totalSockets = len(iopList)
            # something wrong if 0?
            if totalSockets == 0:
                print "WARNING: is something wrong with this io stats response?"
                print h2o.dump_json(stats)

            print "iostats: Total sockets:", totalSockets
            for i in iopList:
                print "iostats:", i[1]

if __name__ == '__main__':
    h2o.unit_main()
