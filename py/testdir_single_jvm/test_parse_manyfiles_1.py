import unittest, time, sys, random
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts, h2o_glm, h2o_browse as h2b, h2o_import as h2i, h2o_exec as h2e

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        # assume we're at 0xdata with it's hdfs namenode
        global localhost
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(2)
        else:
            # all hdfs info is done thru the hdfs_config michal's ec2 config sets up?
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_parse_manyfiles_1(self):
        h2o.beta_features = True
        # these will be used as directory imports/parse
        csvDirname = "manyfiles-nflx-gz"
        timeoutSecs = 600
        trial = 0
        for delete_on_done in [0, 0]:
            
            csvFilename = "file_1.dat.gz"
            csvPathname = csvDirname + "/" + csvFilename
            trialStart = time.time()
            # PARSE****************************************
            hex_key =  csvFilename + "_" + str(trial) + ".hex"
            start = time.time()
            parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathname, schema='local', hex_key=hex_key,
                delete_on_done=delete_on_done,
                timeoutSecs=timeoutSecs, retryDelaySecs=10, pollTimeoutSecs=120, doSummary=False)
            elapsed = time.time() - start
            print "parse end on ", parseResult['destination_key'], 'took', elapsed, 'seconds',\
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)

            # INSPECT******************************************
            # We should be able to see the parse result?
            start = time.time()
            inspect = h2o_cmd.runInspect(None, parseResult['destination_key'], timeoutSecs=360)
            print "Inspect:", parseResult['destination_key'], "took", time.time() - start, "seconds"
            h2o_cmd.infoFromInspect(inspect, csvPathname)
            numRows = inspect['numRows']
            numCols = inspect['numCols']
            self.assertEqual(numCols, 542)
            self.assertEqual(numRows, 100000)

            # gives us some reporting on missing values, constant values, to see if we have x specified well
            # figures out everything from parseResult['destination_key']
            # needs y to avoid output column (which can be index or name)
            # assume all the configs have the same y..just check with the firs tone
            goodX = h2o_glm.goodXFromColumnInfo(y=54, key=parseResult['destination_key'], timeoutSecs=300)

            # STOREVIEW***************************************
            print "\nTrying StoreView after the parse"
            h2o_cmd.runStoreView(timeoutSecs=120)

            # convert to binomial
            execExpr="A.hex=%s" % parseResult['destination_key']
            h2e.exec_expr(execExpr=execExpr, timeoutSecs=60)

            execExpr = 'A.hex[,378+1]=(A.hex[,378+1]>15)'
            h2e.exec_expr(execExpr=execExpr, timeoutSecs=60)

            aHack = {'destination_key': "A.hex"}


            print "Trial #", trial, "completed in", time.time() - trialStart, "seconds."
            trial += 1

if __name__ == '__main__':
    h2o.unit_main()
