import unittest, time, random, sys
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts, h2o_glm
import h2o_browse as h2b
import h2o_import as h2i
import json

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

    def test_storeview_import(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        importFolderPath = "/home/0xdiag/datasets/standard"
        csvFilelist = [
            ("covtype.data", 300),
        ]
        # IMPORT**********************************************
        # H2O deletes the source key. So re-import every iteration if we re-use the src in the list
        importFolderResult = h2i.setupImportFolder(None, importFolderPath)
        ### print "importHDFSResult:", h2o.dump_json(importFolderResult)
        # the list could be from hdfs/s3 (ec2 remap) or local. They have to different list structures
        if 'succeeded' in importFolderResult:
            succeededList = importFolderResult['succeeded']
        elif 'files' in importFolderResult:
            succeededList = importFolderResult['files']
        else:
            raise Exception ("Can't find 'files' or 'succeeded' in import list")

        ### print "succeededList:", h2o.dump_json(succeededList)

        self.assertGreater(len(succeededList),3,"Should see more than 3 files in the import?")
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
            parseResult = h2i.parseImportFolderFile(None, csvFilename, importFolderPath,
                key2=key2, timeoutSecs=timeoutSecs)
            elapsed = time.time() - start
            print "parse end on ", csvFilename, 'took', elapsed, 'seconds',\
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)
            print "parse result:", parseResult['destination_key']

            # INSPECT******************************************
            start = time.time()
            inspect = h2o_cmd.runInspect(None, parseResult['destination_key'], timeoutSecs=360)
            print "Inspect:", parseResult['destination_key'], "took", time.time() - start, "seconds"
            h2o_cmd.infoFromInspect(inspect, csvPathname)

            # SUMMARY****************************************
            # gives us some reporting on missing values, constant values, 
            # to see if we have x specified well
            # figures out everything from parseResult['destination_key']
            # needs y to avoid output column (which can be index or name)
            # assume all the configs have the same y..just check with the firs tone
            goodX = h2o_glm.goodXFromColumnInfo(y=0,
                key=parseResult['destination_key'], timeoutSecs=300)
            summaryResult = h2o_cmd.runSummary(key=key2, timeoutSecs=360)
            h2o_cmd.infoFromSummary(summaryResult, noPrint=True)

            # STOREVIEW***************************************
            print "Trying StoreView to all nodes after the parse"
            
            for n, node in enumerate(h2o.nodes):
                print "\n*****************"
                print "StoreView node %s:%s" % (node.http_addr, node.port)
                storeViewResult = h2o_cmd.runStoreView(node, timeoutSecs=30)
                f = open(SYNDATASETS_DIR + "/storeview_" + str(n) + ".txt", "w" )
                result = json.dump(storeViewResult, f, indent=4, sort_keys=True, default=str)
                f.close()
                lastStoreViewResult = storeViewResult
            

            print "Trial #", trial, "completed in", time.time() - trialStart, "seconds."
            trial += 1

if __name__ == '__main__':
    h2o.unit_main()
