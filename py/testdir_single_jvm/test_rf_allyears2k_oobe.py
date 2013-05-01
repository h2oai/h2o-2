import unittest
import random, sys, time
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_rf as h2f, h2o_hosts
import h2o_import as h2i

# output is last col (we created the last two cols at 0xdata? not in orig dataset)
paramDict = {
    # 'response_variable': 'IsDepDelayed',
    'response_variable': 30,
    'ntree': 50,
    'model_key': 'model_keyA',
    'out_of_bag_error_estimate': 1,
    # 'class_weight': None,
    # 'sample': 66,
    # 'stat_type': 'ENTROPY',
    # 'depth': 2147483647, 
    # 'bin_limit': 10000,
    # 'parallel': 1,
    ## 'seed': 3,
    ## 'features': 30,
    ## 'exclusive_split_limit': 0,
    }

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global localhost
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(node_count=3, java_heap_GB=4)
        else:
            h2o_hosts.build_cloud_with_hosts(node_count=1, java_heap_GB=4)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_rf_allyears2k_oobe(self):
        importFolderPath = '/home/0xdiag/datasets'
        csvFilename = 'allyears2k.csv'
        csvPathname = importFolderPath + "/" + csvFilename
        h2i.setupImportFolder(None, importFolderPath)
        parseKey = h2i.parseImportFolderFile(None, csvFilename, importFolderPath, timeoutSecs=60)
        inspect = h2o_cmd.runInspect(None, parseKey['destination_key'])
        h2o_cmd.infoFromInspect(inspect, csvPathname)

        for trial in range(10):
            kwargs = paramDict
            timeoutSecs = 30 + kwargs['ntree'] * 2

            start = time.time()
            # randomize the node
            node = h2o.nodes[random.randint(0,len(h2o.nodes)-1)]
            rfView = h2o_cmd.runRFOnly(node=node, parseKey=parseKey, timeoutSecs=timeoutSecs, **kwargs)
            elapsed = time.time() - start
            print "RF end on ", csvPathname, 'took', elapsed, 'seconds.', \
                "%d pct. of timeout" % ((elapsed/timeoutSecs) * 100)

            classification_error = rfView['confusion_matrix']['classification_error']
            rows_skipped = rfView['confusion_matrix']['rows_skipped']
            mtry = rfView['mtry']
            mtry_nodes = rfView['mtry_nodes']
            print "mtry:", mtry
            print "mtry_nodes:", mtry_nodes
            self.assertEqual(classification_error, 0, "Should have zero oobe error")
            self.assertEqual(rows_skipped, 39, "Should have exactly 39 rows skipped")

            print "Trial #", trial, "completed"

if __name__ == '__main__':
    h2o.unit_main()
