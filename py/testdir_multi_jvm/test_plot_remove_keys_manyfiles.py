import unittest, random, sys, time
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts, h2o_browse as h2b, h2o_import as h2i, h2o_exec as h2e, h2o_util, h2o_gbm

print "Plot the time to remove a key vs parsed size"
class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED, localhost
        SEED = h2o.setup_random_seed()
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(3,java_heap_GB=4)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_plot_remove_keys_manyfiles(self):
        h2o.beta_features = True
        SYNDATASETS_DIR = h2o.make_syn_dir()

        print "Remember, the parse only deletes what got parsed. We import the folder. So we double import. That should work now"
        tryList = [
            ("file_1[0-9].dat.gz", 'c10', 400),
            ("file_[1-2][0-9].dat.gz", 'c20', 400),
            ("file_[1-4][0-9].dat.gz", 'c40', 400),
            ("file_[1-8][0-9].dat.gz", 'c80', 400),
            ("file_[1-2][1-8][0-9].dat.gz", 'c160', 400),
        ]
        
        xList = []
        eList = []
        fList = []
        importFolderPath = "manyfiles-nflx-gz"
        for (csvFilePattern, hex_key, timeoutSecs) in tryList:
            SEEDPERFILE = random.randint(0, sys.maxint)

            csvPathname = importFolderPath + "/" + csvFilePattern
            start = time.time()
            parseResult = h2i.import_parse(bucket="home-0xdiag-datasets", path=csvPathname, hex_key=hex_key, 
                retryDelaySecs=3, timeoutSecs=timeoutSecs, doSummary=False)
            parseElapsed = time.time() - start
            print "Parse only:", parseResult['destination_key'], "took", parseElapsed, "seconds"
            h2o.check_sandbox_for_errors()

            # We should be able to see the parse result?
            start = time.time()
            inspect = h2o_cmd.runInspect(None, parseResult['destination_key'], timeoutSecs=timeoutSecs2)
            print "Inspect:", parseResult['destination_key'], "took", time.time() - start, "seconds"
            h2o_cmd.infoFromInspect(inspect, csvPathname)
            print "\n" + csvPathname, \
                "    numRows:", "{:,}".format(inspect['numRows']), \
                "    numCols:", "{:,}".format(inspect['numCols'])

            # should match # of cols in header or ??
            self.assertEqual(inspect['numCols'], colCount,
                "parse created result with the wrong number of cols %s %s" % (inspect['numCols'], colCount))
            self.assertEqual(inspect['numRows'], rowCount,
                "parse created result with the wrong number of rows (header shouldn't count) %s %s" % \
                (inspect['numRows'], rowCount))

            parsedBytes = inspect['byteSize']

            node = h2o.nodes[0]
            print "Deleting", hex_key, "at", node.http_addr, "Shouldn't matter what node the delete happens at..global?"
            start = time.time()
            node.remove_key(hex_key, timeoutSecs=30)
            removeElapsed = time.time() - start
            print "Deleting", hex_key, "took", removeElapsed, "seconds"

            # xList.append(ntrees)
            xList.append(parsedBytes)
            eList.append(parseElapsed)
            fList.append(removeElapsed)

        # just plot the last one
        if 1==1:
            xLabel = 'parsedBytes'
            eLabel = 'parseElapsed'
            fLabel = 'removeElapsed'
            eListTitle = ""
            fListTitle = ""
            h2o_gbm.plotLists(xList, xLabel, eListTitle, eList, eLabel, fListTitle, fList, fLabel)

if __name__ == '__main__':
    h2o.unit_main()
