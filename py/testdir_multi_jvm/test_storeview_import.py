import unittest, time, random, sys
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts, h2o_glm
import h2o_browse as h2b
import h2o_import as h2i

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        # assume we're at 0xdata with it's hdfs namenode
        global localhost
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(3)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_parse_storeview_import(self):
        importFolderPath = "/home/0xdiag/datasets/standard"
        csvFilelist = [
            ("covtype.data", 300),
        ]
        # IMPORT**********************************************
        # since H2O deletes the source key, we should re-import every iteration if we re-use the src in the list
        importFolderResult = h2i.setupImportFolder(None, importFolderPath)
        ### print "importHDFSResult:", h2o.dump_json(importFolderResult)
        succeededList = importFolderResult['succeeded']
        ### print "succeededList:", h2o.dump_json(succeededList)

        self.assertGreater(len(suceededList),3,"Should see more than 3 files in the import?")
        # why does this hang? can't look at storeview after import?
        print "\nTrying StoreView after the import folder"
        h2o_cmd.runStoreView(timeoutSecs=30)

        trial = 0
        for (csvFilename, timeoutSecs) in csvFilelist:
            trialStart = time.time()
            csvPathname = csvFilename

            # PARSE****************************************
            key2 = csvFilename + "_" + str(trial) + ".hex"
            print "parse start on:", csvFilename
            start = time.time()
            parseKey = h2i.parseImportFolderFile(None, csvFilename, importFolderPath,
                key2=key2, timeoutSecs=timeoutSecs)
            elapsed = time.time() - start
            print "parse end on ", csvFilename, 'took', elapsed, 'seconds',\
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)
            print "parse result:", parseKey['destination_key']

            # INSPECT******************************************
            # We should be able to see the parse result?
            start = time.time()
            inspect = h2o_cmd.runInspect(None, parseKey['destination_key'], timeoutSecs=360)
            print "Inspect:", parseKey['destination_key'], "took", time.time() - start, "seconds"
            h2o_cmd.infoFromInspect(inspect, csvPathname)

            # SUMMARY****************************************
            if 1==0:
                # gives us some reporting on missing values, constant values, 
                # to see if we have x specified well
                # figures out everything from parseKey['destination_key']
                # needs y to avoid output column (which can be index or name)
                # assume all the configs have the same y..just check with the firs tone
                goodX = h2o_glm.goodXFromColumnInfo(y=0,
                    key=parseKey['destination_key'], timeoutSecs=300)
                summaryResult = h2o.nodes[0].summary_page(key2, timeoutSecs=360)
                summary = summaryResult['summary']
                # print h2o.dump_json(summary)
                infoFromSummary(self, summary)

            # STOREVIEW***************************************
            print "Trying StoreView after the parse"
            h2o_cmd.runStoreView(timeoutSecs=30)

            print "Trial #", trial, "completed in", time.time() - trialStart, "seconds."
            trial += 1

if __name__ == '__main__':
    h2o.unit_main()
