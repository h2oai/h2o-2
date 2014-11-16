import unittest, sys, random, time
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i, h2o_jobs

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        print "Will build clouds with incrementing heap sizes and import folder/parse"

    @classmethod
    def tearDownClass(cls):
        # the node state is gone when we tear down the cloud, so pass the ignore here also.
        h2o.tear_down_cloud(sandboxIgnoreErrors=True)

    def test_flashgordon(self):
        # typical size of the michal files
        avgMichalSize = 116561140
        avgSynSize = 4020000
        csvFilenameList = [
            ("100.dat.gz", "dat_1", 1 * avgSynSize, 700),
            ("11[0-9].dat.gz", "dat_10", 10 * avgSynSize, 700),
            ("1[32][0-9].dat.gz", "dat_20", 20 * avgSynSize, 800),
            ("1[5-9][0-9].dat.gz", "dat_50", 50 * avgSynSize, 900),
            # ("1[0-9][0-9].dat.gz", "dat_100", 100 * avgSynSize, 1200),
        ]

        print "Using the -.gz files from s3"
    
        USE_S3 = False
        noPoll = True
        benchmarkLogging = ['cpu','disk']
        bucket = "flashgordon"
        trialMax = 1
        # use i to forward reference in the list, so we can do multiple outstanding parses below
        for i, (csvFilepattern, csvFilename, totalBytes, timeoutSecs) in enumerate(csvFilenameList):
            ## for tryHeap in [54, 28]:
            for tryHeap in [28]:
                
                print "\n", tryHeap,"GB heap, 1 jvm per host, import", protocol, "then parse"
                h2o.init(java_heap_GB=tryHeap, enable_benchmark_log=True, timeoutSecs=120, retryDelaySecs=10)

                # don't raise exception if we find something bad in h2o stdout/stderr?
                h2o.nodes[0].sandboxIgnoreErrors = True

                for trial in range(trialMax):
                    # since we delete the key, we have to re-import every iteration, to get it again
                    # s3n URI thru HDFS is not typical.
                    if USE_S3:
                        schema = 's3'
                    else:
                        schema = 's3n'

                    hex_key = csvFilename + "_" + str(trial) + ".hex"
                    start = time.time()
                    parseResult = h2i.import_parse(bucket=bucket, path=csvFilepattern, schema=schema, hex_key=hex_key,
                        timeoutSecs=timeoutSecs, retryDelaySecs=10, pollTimeoutSecs=60,
                        noPoll=noPoll,
                        benchmarkLogging=benchmarkLogging)

                    if noPoll:
                        time.sleep(1)
                        h2o.check_sandbox_for_errors()
                        (csvFilepattern, csvFilename, totalBytes2, timeoutSecs) = csvFilenameList[i+1]
                        hex_key = csvFilename + "_" + str(trial) + ".hex"
                        parseResult = h2i.import_parse(bucket=bucket, path=csvFilepattern, schema=schema, hex_key=hex_key,
                            timeoutSecs=timeoutSecs, retryDelaySecs=10, pollTimeoutSecs=60,
                            noPoll=noPoll,
                            benchmarkLogging=benchmarkLogging)

                        time.sleep(1)
                        h2o.check_sandbox_for_errors()
                        (csvFilepattern, csvFilename, totalBytes3, timeoutSecs) = csvFilenameList[i+2]
                        s3nKey = URI + csvFilepattern
                        hex_key = csvFilename + "_" + str(trial) + ".hex"
                        parseResult = h2i.import_parse(bucket=bucket, path=csvFilepattern, schema=schema, hex_key=hex_key,
                            timeoutSecs=timeoutSecs, retryDelaySecs=10, pollTimeoutSecs=60,
                            noPoll=noPoll,
                            benchmarkLogging=benchmarkLogging)

                    elapsed = time.time() - start
                    print "parse result:", parseResult['destination_key']
                    print "Parse #", trial, "completed in", "%6.2f" % elapsed, "seconds.", \
                        "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)

                    # print stats on all three if noPoll
                    if noPoll:
                        # does it take a little while to show up in Jobs, from where we issued the parse?
                        time.sleep(2)
                        # FIX! use the last (biggest?) timeoutSecs? maybe should increase since parallel
                        h2o_jobs.pollWaitJobs(pattern=csvFilename, 
                            timeoutSecs=timeoutSecs, benchmarkLogging=benchmarkLogging)
                        # for getting the MB/sec closer to 'right'
                        totalBytes += totalBytes2 + totalBytes3
                        elapsed = time.time() - start
                        h2o.check_sandbox_for_errors()

                    if totalBytes is not None:
                        fileMBS = (totalBytes/1e6)/elapsed
                        print "\nMB/sec (before uncompress)", "%6.2f" % fileMBS
                        h2o.cloudPerfH2O.message('{:d} jvms, {:d}GB heap, {:s} {:s} {:6.2f} MB/sec for {:6.2f} secs'.format(
                            len(h2o.nodes), tryHeap, csvFilepattern, csvFilename, fileMBS, elapsed))

                    # BUG here?
                    if not noPoll:
                        # We should be able to see the parse result?
                        inspect = h2o_cmd.runInspect(key=parseResult['destination_key'])

                h2o.tear_down_cloud()
                # sticky ports? wait a bit.
                time.sleep(120)

if __name__ == '__main__':
    h2o.unit_main()
