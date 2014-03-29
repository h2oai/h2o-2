import unittest, sys, random, time
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i, h2o_hosts, h2o_jobs, h2o_exec as h2e

print "Back to Basics!"
DO_EXEC_QUANT = True
thresholds = "0,0.001,0.999"

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        print "Will build_cloud() with random heap size and do overlapped import folder/parse (groups)"
        global SEED, localhost
        SEED = h2o.setup_random_seed()
        tryHeap = random.randint(4,28)
        # print "\n", tryHeap,"GB heap, 1 jvm per host, import 192.168.1.176 hdfs, then parse"
        print "\n", tryHeap,"GB heap, 1 jvm per host, import,  then parse"
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(node_count=1, java_heap_GB=tryHeap,
                # use_hdfs=True, hdfs_name_node='192.168.1.176', hdfs_version='cdh3'
            )
        else:
            h2o_hosts.build_cloud_with_hosts(node_count=1, java_heap_GB=tryHeap,
                # use_hdfs=True, hdfs_name_node='192.168.1.176', hdfs_version='cdh3'
            )

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_parse_nflx_loop_hdfs_fvec(self):
        h2o.beta_features = True
        # hdfs://<name node>/datasets/manyfiles-nflx-gz/file_1.dat.gz
        importFolderPath = "standard"
        csvFilename = "covtype.data"
        csvFilePattern = "covtype.data"

        # don't raise exception if we find something bad in h2o stdout/stderr?
        # h2o.nodes[0].sandboxIgnoreErrors = True
        trialMax = 100
        parseTrial = 0
        summaryTrial = 0
        timeoutSecs = 500
        outstanding = 5

        # okay to reuse the src_key name. h2o deletes? use unique hex to make sure it's not reused.
        # might go to unique src keys also ..oops have to, to prevent complaints about the key (lock)
        # can't repeatedly import the folder

        # only if not noPoll. otherwise parse isn't done
        # I guess I have to use 'put' so I can name the src key unique, to get overlap
        # I could tell h2o to not delete, but it's nice to get the keys in a new place?
        # maybe rebalance? FIX! todo

        while parseTrial <= trialMax:
            start = time.time()
            for o in range(outstanding):
                parseTrial += 1
                src_key = csvFilename + "_" + str(parseTrial)
                hex_key = csvFilename + "_" + str(parseTrial) + ".hex"
                # "key": "hdfs://192.168.1.176/datasets/manyfiles-nflx-gz/file_99.dat.gz", 
                csvPathname = importFolderPath + "/" + csvFilePattern
                start = time.time()

                (importResult, importPattern) = h2i.import_only(bucket='home-0xdiag-datasets', path=csvPathname, schema='put', 
                    src_key=src_key,
                    timeoutSecs=timeoutSecs, retryDelaySecs=10, pollTimeoutSecs=60)

                # don't need doSummary=False, unlike import_parse()
                parseResult = h2i.parse_only(pattern=importPattern, hex_key=hex_key,
                    timeoutSecs=timeoutSecs, retryDelaySecs=10, pollTimeoutSecs=60, 
                    noPoll=True)

            # h2o_jobs.pollWaitJobs(timeoutSecs=180)
            h2o_jobs.pollStatsWhileBusy(timeoutSecs=300, pollTimeoutSecs=15, retryDelaySecs=0.25)

            elapsed = time.time() - start
            print "Parse group end at #", parseTrial, "completed in", "%6.2f" % elapsed, "seconds.", \
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)

            # do last to first..to get race condition?
            o = summaryTrial + outstanding
            for p in range(outstanding):
                summaryTrial += 1
                hex_key = csvFilename + "_" + str(o) + ".hex"

                if DO_EXEC_QUANT:
                    execExpr = "r2=c(1); r2=quantile(%s[,1], c(%s));" % (hex_key, thresholds)
                    (resultExec, result) = h2e.exec_expr(execExpr=execExpr, timeoutSecs=30)
                else:
                    h2o_cmd.runSummary(key=hex_key)

                o -= 1

                 

            h2o_cmd.runStoreView()


if __name__ == '__main__':
    h2o.unit_main()
