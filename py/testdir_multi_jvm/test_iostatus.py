import os, json, unittest, time, shutil, sys
sys.path.extend(['.','..','py'])

import h2o, h2o_hosts

class JStackApi(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(3,sigar=True)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_iostatus(self):
        # Ask each node for iostatus statistics
        for n in h2o.nodes:
            stats = n.iostatus()
            h2o.verboseprint(json.dumps(stats,indent=2))

            histogram = stats['histogram'] 
            # print the last one (largest window)
            # print h2o.dump_json(histogram[0])
            for k in histogram:
                print k

            raw_iops = stats['raw_iops'] 
            for k in raw_iops:
                print k

if __name__ == '__main__':
    h2o.unit_main()
