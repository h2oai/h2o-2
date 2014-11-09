import unittest, sys, random, time
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i, h2o_jobs, h2o_exec as h2e
import h2o_util

import multiprocessing, os, signal, time
from multiprocessing import Process, Queue, Pool

print "Back to Basics with a multiprocessing twist!"
DO_EXEC_QUANT = False
DO_SUMMARY = True
DO_XORSUM = False

DO_BIGFILE = True
DO_IRIS = True

# overrides the calc below if not None

DO_PARSE_ALSO = True
UPLOAD_PARSE_DIFF_NODES = True
RANDOM_HEAP = False

PARSE_NOPOLL = False

thresholdsList = [0.5]
thresholds = ",".join(map(str, thresholdsList))

# problem with keyboard interrupt described
# http://bryceboe.com/2012/02/14/python-multiprocessing-pool-and-keyboardinterrupt-revisited/
def function_no_keyboard_intr(result_queue, function, *args):
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    result_queue.put(function(*args))

def parseit(n, pattern, hex_key, timeoutSecs=60, retryDelaySecs=1, pollTimeoutSecs=30):
    h2i.parse_only(node=h2o.nodes[n], pattern=pattern, hex_key=hex_key,
        timeoutSecs=timeoutSecs, retryDelaySecs=retryDelaySecs, pollTimeoutSecs=pollTimeoutSecs, 
        noPoll=PARSE_NOPOLL)
    print pattern, "started in parseit (nopoll)"
    return 'Done'

def uploadit(n, bucket, path, src_key, hex_key, timeoutSecs=60, retryDelaySecs=1, pollTimeoutSecs=30):
    # apparently the putfile has some conflicts. but afte the put completes, its okay
    # to be parallel with the src_key if it has a different name
    (importResult, importPattern) = h2i.import_only(node=h2o.nodes[n],
        bucket=bucket, path=path, schema='put', 
        src_key=src_key,
        timeoutSecs=timeoutSecs, retryDelaySecs=10, pollTimeoutSecs=60)
    print "uploadit:", importPattern, hex_key
    # do the parse on the next node
    if UPLOAD_PARSE_DIFF_NODES:
        np1 = (n+1) % len(h2o.nodes)
    else:
        np1 = n
    if DO_PARSE_ALSO:
        parseit(np1, importPattern, hex_key, 
            timeoutSecs=timeoutSecs, retryDelaySecs=retryDelaySecs, pollTimeoutSecs=pollTimeoutSecs)
        h2o.nodes[0].rebalance(source=hex_key, after=hex_key + "_2", chunks=32)
    return (importPattern, hex_key)


pool = Pool(16)

class Basic(unittest.TestCase):
    def tearDown(self):
        pool.close()
        # pool.join()
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        print "Will build_cloud() with random heap size and do overlapped import folder/parse (groups)"
        global SEED
        SEED = h2o.setup_random_seed()
        if RANDOM_HEAP:
            tryHeap = random.randint(4,28)
        else:
            tryHeap = 28

        # print "\n", tryHeap,"GB heap, 1 jvm per host, import 172.16.2.176 hdfs, then parse"
        print "\n", tryHeap,"GB heap, 1 jvm per host, import,  then parse"
        h2o.init(node_count=3, java_heap_GB=4)
                # use_hdfs=True, hdfs_name_node='172.16.2.176', hdfs_version='cdh4'

    @classmethod
    def tearDownClass(cls):
        pool.close()
        # pool.join()
        h2o.tear_down_cloud()

    def test_parse_multiprocess_fvec(self):
        # hdfs://<name node>/datasets/manyfiles-nflx-gz/file_1.dat.gz
        # don't raise exception if we find something bad in h2o stdout/stderr?
        # h2o.nodes[0].sandboxIgnoreErrors = True
        OUTSTANDING = min(10, len(h2o.nodes))

        if DO_IRIS:
            global DO_BIGFILE
            DO_BIGFILE = False
            bucket = 'smalldata'
            importFolderPath = "iris"
            csvFilename = "iris2.csv"
            csvFilePattern = "iris2.csv"
            trialMax = 20

        elif DO_BIGFILE:
            bucket = 'home-0xdiag-datasets'
            importFolderPath = "standard"
            csvFilename = "covtype20x.data"
            csvFilePattern = "covtype20x.data"
            trialMax = 2 * OUTSTANDING
        else:
            bucket = 'home-0xdiag-datasets'
            importFolderPath = "standard"
            csvFilename = "covtype.data"
            csvFilePattern = "covtype.data"
            trialMax = 40 * OUTSTANDING

        # add one just to make it odd
        # OUTSTANDING = min(10, len(h2o.nodes) + 1)
        # don't have more than one source file per node OUTSTANDING? (think of the node increment rule)
    
        # okay to reuse the src_key name. h2o deletes? use unique hex to make sure it's not reused.
        # might go to unique src keys also ..oops have to, to prevent complaints about the key (lock)
        # can't repeatedly import the folder

        # only if not noPoll. otherwise parse isn't done
        # I guess I have to use 'put' so I can name the src key unique, to get overlap
        # I could tell h2o to not delete, but it's nice to get the keys in a new place?
        # maybe rebalance? FIX! todo

        parseTrial = 0
        summaryTrial = 0
        uploader_resultq = multiprocessing.Queue()
        while parseTrial <= trialMax:
            start = time.time()
            uploaders = []
            if not DO_IRIS:
                assert OUTSTANDING<=10 , "we only have 10 links with unique names to covtype.data"
            for o in range(OUTSTANDING):
                src_key = csvFilename + "_" + str(parseTrial) 
                hex_key = csvFilename + "_" + str(parseTrial) + ".hexxx"
                # "key": "hdfs://172.16.2.176/datasets/manyfiles-nflx-gz/file_99.dat.gz", 

                # hacked hard ln so source keys would have different names? was getting h2o locking issues
                if DO_IRIS:
                    csvPathname = importFolderPath + "/" + csvFilePattern
                else:
                    csvPathname = importFolderPath + "/" + csvFilePattern + "_" + str(o)
                start = time.time()

                # walk the nodes
                # if this rule is matched for exec/summary below, it should find the name okay? (npe with xorsum)
                # summary2 not seeing it?
                np = parseTrial % len(h2o.nodes)
                retryDelaySecs=5 if DO_BIGFILE else 1
                timeoutSecs=60 if DO_BIGFILE else 15
                tmp = multiprocessing.Process(target=function_no_keyboard_intr,
                    args=(uploader_resultq, uploadit, np, bucket, csvPathname, src_key, hex_key, timeoutSecs, retryDelaySecs))
                tmp.start()
                uploaders.append(tmp)
                parseTrial += 1

            # now sync on them
            for uploader in uploaders:
                try:
                    uploader.join()
                    # don't need him any more
                    uploader.terminate()
                    (importPattern, hex_key) = uploader_resultq.get(timeout=10)
                except KeyboardInterrupt:
                    print 'parent received ctrl-c'
                    for uploader in uploaders:
                        uploader.terminate()
                        uploader.join()
            elapsed = time.time() - start
            print "Parse group end at #", parseTrial, "completed in", "%6.2f" % elapsed, "seconds.", \
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)

        print "We might have parses that haven't completed. The join just says we can reuse some files (parse still going)"
        if PARSE_NOPOLL:
            h2o_jobs.pollWaitJobs(timeoutSecs=180)

        h2o_cmd.runStoreView()
        # h2o_jobs.pollStatsWhileBusy(timeoutSecs=300, pollTimeoutSecs=15, retryDelaySecs=0.25)

        if DO_PARSE_ALSO: # only if we parsed
            print "These all go to node [0]"
            # getting a NPE if I do xorsum (any exec?) ..just do summary for now..doesn't seem to have the issue
            # suspect it's about the multi-node stuff above
            for summaryTrial in range(trialMax):

                # do last to first..to get race condition?
                firstXorUll = None
                firstQuantileUll = None
                hex_key = csvFilename + "_" + str(summaryTrial) + ".hexxx"
                
                if DO_EXEC_QUANT:
                    execExpr = "r2=c(1); r2=quantile(%s[,1], c(%s));" % (hex_key, thresholds)
                    (resultExec, fpResult) = h2e.exec_expr(execExpr=execExpr, timeoutSecs=30)
                    ullResult = h2o_util.doubleToUnsignedLongLong(fpResult)
                    print "%30s" % "median ullResult (0.16x):", "0x%0.16x   %s" % (ullResult, fpResult)
                    if firstQuantileUll:
                        self.assertEqual(ullResult, firstQuantileUll)
                    else:
                        firstQuantileUll = ullResult

                if DO_XORSUM:
                    execExpr = "r2=c(1); r2=xorsum(%s[,1], c(%s));" % (hex_key, thresholds)
                    (resultExec, fpResult) = h2e.exec_expr(execExpr=execExpr, timeoutSecs=30)
                    ullResult = h2o_util.doubleToUnsignedLongLong(fpResult)
                    print "%30s" % "xorsum ullResult (0.16x):", "0x%0.16x   %s" % (ullResult, fpResult)

                    if firstXorUll:
                        self.assertEqual(ullResult, firstXorUll)
                    else:
                        firstXorUll = ullResult

                if DO_SUMMARY:
                    h2o_cmd.runSummary(key=hex_key)


if __name__ == '__main__':
    h2o.unit_main()
