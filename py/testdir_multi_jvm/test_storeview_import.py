import unittest, time, random, sys
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_glm, h2o_browse as h2b, h2o_import as h2i

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        # assume we're at 0xdata with it's hdfs namenode
        h2o.init(1)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_storeview_import(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        importFolderPath = "standard"
        csvFilelist = [
            ("covtype.data", 300),
        ]

        trial = 0
        for (csvFilename, timeoutSecs) in csvFilelist:
            csvPathname = importFolderPath + "/" + csvFilename
            trialStart = time.time()

            # PARSE****************************************
            importResult = h2i.import_only(bucket='home-0xdiag-datasets', path="*", timeoutSecs=timeoutSecs)
            print h2o.dump_json(importResult)
            storeViewResult = h2o_cmd.runStoreView(timeoutSecs=30)
            # print h2o.dump_json(storeViewResult)

            hex_key = csvFilename + "_" + str(trial) + ".hex"
            print "parse start on:", csvFilename
            start = time.time()
            parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathname, schema='local',
                hex_key=hex_key, timeoutSecs=timeoutSecs)
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
            summaryResult = h2o_cmd.runSummary(key=hex_key, timeoutSecs=360)
            h2o_cmd.infoFromSummary(summaryResult, noPrint=True)

            # STOREVIEW***************************************
            print "Trying StoreView to all nodes after the parse"
            
            for n, node in enumerate(h2o.nodes):
                print "\n*****************"
                print "StoreView node %s:%s" % (node.http_addr, node.port)
                storeViewResult = h2o_cmd.runStoreView(node, timeoutSecs=30)
                f = open(SYNDATASETS_DIR + "/storeview_" + str(n) + ".txt", "w" )
                result = h2o.dump_json(storeViewResult)
                f.close()
                lastStoreViewResult = storeViewResult
            

            print "Trial #", trial, "completed in", time.time() - trialStart, "seconds."
            trial += 1

if __name__ == '__main__':
    h2o.unit_main()
