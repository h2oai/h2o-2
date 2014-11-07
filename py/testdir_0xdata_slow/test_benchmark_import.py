import unittest, time, sys, random, logging
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i, h2o_glm, h2o_exec as h2e, h2o_jobs

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        pass

    @classmethod
    def tearDownClass(cls):
        ### time.sleep(3600)
        h2o.tear_down_cloud()

    def test_benchmark_import(self):
        # typical size of the michal files
        avgMichalSizeUncompressed = 237270000 
        avgMichalSize = 116561140 
        avgSynSize = 4020000
        covtype200xSize = 15033863400
        synSize =  183
        if 1==1:
            bucket = 'home-0xdiag-datasets'
            importFolderPath = 'manyfiles-nflx-gz'
            print "Using .gz'ed files in", importFolderPath
            csvFilenameAll = [
                # this should hit the "more" files too?
                # ("*.dat.gz", "file_200.dat.gz", 1200 * avgMichalSize, 1800),
                # ("*.dat.gz", "file_200.dat.gz", 1200 * avgMichalSize, 1800),
                # ("*[1][0-2][0-9].dat.gz", "file_30.dat.gz", 50 * avgMichalSize, 1800), 
                # ("*file_[0-9][0-9].dat.gz", "file_100.dat.gz", 100 * avgMichalSize, 1800), 
                ("*file_[12][01][0-7].dat.gz", "file_32.dat.gz", 100 * avgMichalSize, 1800), 
                # ("*file_[12][0-9][0-9].dat.gz", "file_200_A.dat.gz", 200 * avgMichalSize, 1800), 

                # ("*file_*.dat.gz", "file_300_A.dat.gz", 300 * avgMichalSize, 1800), 
                # ("*file_[34][0-9][0-9].dat.gz", "file_200_B.dat.gz", 200 * avgMichalSize, 1800), 
                # ("*file_[56][0-9][0-9].dat.gz", "file_200_C.dat.gz", 200 * avgMichalSize, 1800), 
                # ("*file_[78][0-9][0-9].dat.gz", "file_200_D.dat.gz", 200 * avgMichalSize, 1800), 
                # ("*.dat.gz", "file_1200.dat.gz", 1200 * avgMichalSize, 3600),
            ]

        if 1==0:
            bucket = 'home-0xdiag-datasets'
            importFolderPath = 'more1_1200_link'
            print "Using .gz'ed files in", importFolderPath
            csvFilenameAll = [
                # this should hit the "more" files too?
                # ("*10[0-9].dat.gz", "file_10.dat.gz", 10 * avgMichalSize, 3600), 
                # ("*1[0-4][0-9].dat.gz", "file_50.dat.gz", 50 * avgMichalSize, 3600), 
                # ("*[1][0-9][0-9].dat.gz", "file_100.dat.gz", 100 * avgMichalSize, 3600), 
                # ("*3[0-9][0-9].dat.gz", "file_100.dat.gz", 100 * avgMichalSize, 3600),
                # ("*1[0-9][0-9].dat.gz", "file_100.dat.gz", 100 * avgMichalSize, 1800), 
                #("*[1-2][0-9][0-9].dat.gz", "file_200.dat.gz", 200 * avgMichalSize, 3600), 
                # ("*[3-4][0-9][0-9].dat.gz", "file_200.dat.gz", 200 * avgMichalSize, 3600),
                ("*[3-4][0-4][0-9].dat.gz", "file_100_A.dat.gz", 100 * avgMichalSize, 3600),
                ("*[3-4][0-4][0-9].dat.gz", "file_100_B.dat.gz", 100 * avgMichalSize, 3600),

                ("*[3-4][0-5][0-9].dat.gz", "file_120_A.dat.gz", 120 * avgMichalSize, 3600),
                ("*[3-4][0-5][0-9].dat.gz", "file_120_B.dat.gz", 120 * avgMichalSize, 3600),

                ("*[3-4][0-6][0-9].dat.gz", "file_140_A.dat.gz", 140 * avgMichalSize, 3600),
                ("*[3-4][0-6][0-9].dat.gz", "file_140_B.dat.gz", 140 * avgMichalSize, 3600),

                ("*[3-4][0-7][0-9].dat.gz", "file_160_A.dat.gz", 160 * avgMichalSize, 3600),
                ("*[3-4][0-7][0-9].dat.gz", "file_160_B.dat.gz", 160 * avgMichalSize, 3600),

                ("*[3-4][0-8][0-9].dat.gz", "file_180_A.dat.gz", 180 * avgMichalSize, 3600),
                ("*[3-4][0-8][0-9].dat.gz", "file_180_B.dat.gz", 180 * avgMichalSize, 3600),

                ("*[3-4][0-9][0-9].dat.gz", "file_200_A.dat.gz", 200 * avgMichalSize, 3600),
                ("*[3-4][0-9][0-9].dat.gz", "file_200_B.dat.gz", 200 * avgMichalSize, 3600),

                ("*[3-5][0-9][0-9].dat.gz", "file_300.dat.gz", 300 * avgMichalSize, 3600),
                ("*[3-5][0-9][0-9].dat.gz", "file_300.dat.gz", 300 * avgMichalSize, 3600),
                # for now, take too long on 2x100GB heap on 164
                # ("*[3-6][0-9][0-9].dat.gz", "file_400.dat.gz", 400 * avgMichalSize, 3600),
                # ("*[3-6][0-9][0-9].dat.gz", "file_400.dat.gz", 400 * avgMichalSize, 3600),
                # ("*[3-6][0-9][0-9].dat.gz", "file_400.dat.gz", 400 * avgMichalSize, 3600),
                # ("*[3-6][0-9][0-9].dat.gz", "file_400.dat.gz", 400 * avgMichalSize, 3600),
                # ("*[3-6][0-9][0-9].dat.gz", "file_400.dat.gz", 400 * avgMichalSize, 3600),
                # ("*[3-6][0-9][0-9].dat.gz", "file_400.dat.gz", 400 * avgMichalSize, 3600),
                # ("*[3-6][0-9][0-9].dat.gz", "file_400.dat.gz", 400 * avgMichalSize, 3600),
                # ("*[3-6][0-9][0-9].dat.gz", "file_400.dat.gz", 400 * avgMichalSize, 3600),
            ]

        if 1==0:
            bucket = 'home-0xdiag-datasets'
            importFolderPath = 'manyfiles-nflx-gz'
            print "Using .gz'ed files in", importFolderPath
            csvFilenameAll = [
                # this should hit the "more" files too?
                ("*_[123][0-9][0-9]*.dat.gz", "file_300.dat.gz", 300 * avgMichalSize, 3600),
                ("*_[1][5-9][0-9]*.dat.gz", "file_100.dat.gz", 50 * avgMichalSize, 3600),
            ]

        if 1==0: 
            bucket = 'home-0xdiag-datasets'
            importFolderPath = 'standard'
            print "Using .gz'ed files in", importFolderPath
            # all exactly the same prior to gzip!
            # could use this, but remember import folder -> import folder s3 for jenkins?
            # how would it get it right?
            # os.path.getsize(f)
            csvFilenameAll = [
                # ("manyfiles-nflx-gz/file_1[0-9].dat.gz", "file_10.dat.gz", 700),
                # 100 files takes too long on two machines?
                # ("covtype200x.data", "covtype200x.data", 15033863400, 700),
                # I use different files to avoid OS caching effects
                # ("syn_datasets/syn_7350063254201195578_10000x200.csv_000[0-9][0-9]", "syn_100.csv", 100 * avgSynSize, 700),
                # ("syn_datasets/syn_7350063254201195578_10000x200.csv_00000", "syn_1.csv", avgSynSize, 700),
                # ("syn_datasets/syn_7350063254201195578_10000x200.csv_0001[0-9]", "syn_10.csv", 10 * avgSynSize, 700),
                # ("syn_datasets/syn_7350063254201195578_10000x200.csv_000[23][0-9]", "syn_20.csv", 20 * avgSynSize, 700),
                # ("syn_datasets/syn_7350063254201195578_10000x200.csv_000[45678][0-9]", "syn_50.csv", 50 * avgSynSize, 700),
                # ("manyfiles-nflx-gz/file_10.dat.gz", "file_10_1.dat.gz", 1 * avgMichalSize, 700),
                # ("manyfiles-nflx-gz/file_1[0-9].dat.gz", "file_10.dat.gz", 10 * avgMichalSize, 700),

                ("manyfiles-nflx-gz/file_1.dat.gz", "file_1.dat.gz", 1 * avgMichalSize, 700),
                ("manyfiles-nflx-gz/file_[2][0-9].dat.gz", "file_10.dat.gz", 10 * avgMichalSize, 700),
                ("manyfiles-nflx-gz/file_[34][0-9].dat.gz", "file_20.dat.gz", 20 * avgMichalSize, 700),
                ("manyfiles-nflx-gz/file_[5-9][0-9].dat.gz", "file_50.dat.gz", 50 * avgMichalSize, 700),
                ("manyfiles-nflx-gz/file_1[0-9][0-9].dat.gz", "file_100.dat.gz", 50 * avgMichalSize, 700),
                ("manyfiles-nflx-gz/file_[12][0-9][0-9].dat.gz", "file_200.dat.gz", 50 * avgMichalSize, 700),
                ("manyfiles-nflx-gz/file_[12]?[0-9][0-9].dat.gz", "file_300.dat.gz", 50 * avgMichalSize, 700),
                ("manyfiles-nflx-gz/file_*.dat.gz", "file_384.dat.gz", 100 * avgMichalSize, 1200),
                ("covtype200x.data", "covtype200x.data", covtype200xSize, 700),
                ]
        # csvFilenameList = random.sample(csvFilenameAll,1)
        csvFilenameList = csvFilenameAll

        # split out the pattern match and the filename used for the hex
        trialMax = 1
        # rebuild the cloud for each file
        tryHeap = 28
        # can fire a parse off and go wait on the jobs queue (inspect afterwards is enough?)
        DO_GLM = False
        noPoll = False
        # benchmarkLogging = ['cpu','disk', 'iostats', 'jstack']
        # benchmarkLogging = None
        benchmarkLogging = ['cpu','disk', 'network', 'iostats', 'jstack']
        benchmarkLogging = ['cpu','disk', 'network', 'iostats']
        # IOStatus can hang?
        benchmarkLogging = ['cpu', 'disk', 'network']
        pollTimeoutSecs = 120
        retryDelaySecs = 10

        jea = '-XX:MaxDirectMemorySize=512m -XX:+PrintGCDetails' + ' -Dh2o.find-ByteBuffer-leaks'
        jea = '-XX:MaxDirectMemorySize=512m -XX:+PrintGCDetails'
        jea = "-XX:+UseParNewGC -XX:+UseConcMarkSweepGC"
        jea = ' -Dcom.sun.management.jmxremote.port=54330' + \
              ' -Dcom.sun.management.jmxremote.authenticate=false' + \
              ' -Dcom.sun.management.jmxremote.ssl=false'  + \
              ' -Dcom.sun.management.jmxremote' + \
              ' -Dcom.sun.management.jmxremote.local.only=false'
        jea = ' -Dlog.printAll=true'


        for i, (csvFilepattern, csvFilename, totalBytes, timeoutSecs) in enumerate(csvFilenameList):
            h2o.init(2,java_heap_GB=tryHeap, enable_benchmark_log=True)
            # java_extra_args=jea, 

            # pop open a browser on the cloud
            ### h2b.browseTheCloud()

            for trial in range(trialMax):
                csvPathname = importFolderPath + "/" + csvFilepattern
                (importResult, importPattern) = h2i.import_only(bucket=bucket, path=csvPathname, schema='local')
                importFullList = importResult['files']
                importFailList = importResult['fails']
                print "\n Problem if this is not empty: importFailList:", h2o.dump_json(importFailList)
                # creates csvFilename.hex from file in importFolder dir 

                h2o.cloudPerfH2O.change_logfile(csvFilename)
                h2o.cloudPerfH2O.message("")
                h2o.cloudPerfH2O.message("Parse " + csvFilename + " Start--------------------------------")
                csvPathname = importFolderPath + "/" + csvFilepattern
                start = time.time()
                parseResult = h2i.import_parse(bucket=bucket, path=csvPathname, schema='local',
                    hex_key=csvFilename + ".hex", timeoutSecs=timeoutSecs, 
                    retryDelaySecs=retryDelaySecs,
                    pollTimeoutSecs=pollTimeoutSecs,
                    noPoll=noPoll,
                    benchmarkLogging=benchmarkLogging)

                if noPoll:
                    if (i+1) < len(csvFilenameList):
                        time.sleep(1)
                        h2o.check_sandbox_for_errors()
                        (csvFilepattern, csvFilename, totalBytes2, timeoutSecs) = csvFilenameList[i+1]
                        csvPathname = importFolderPath + "/" + csvFilepattern
                        parseResult = h2i.import_parse(bucket=bucket, path=csvPathname, schema='local',
                            hex_key=csvFilename + ".hex", timeoutSecs=timeoutSecs, 
                            retryDelaySecs=retryDelaySecs,
                            pollTimeoutSecs=pollTimeoutSecs,
                            noPoll=noPoll,
                            benchmarkLogging=benchmarkLogging)

                    if (i+2) < len(csvFilenameList):
                        time.sleep(1)
                        h2o.check_sandbox_for_errors()
                        (csvFilepattern, csvFilename, totalBytes3, timeoutSecs) = csvFilenameList[i+2]
                        csvPathname = importFolderPath + "/" + csvFilepattern
                        parseResult = h2i.import_parse(bucket=bucket, path=csvPathname, schema='local',
                            hex_key=csvFilename + ".hex", timeoutSecs=timeoutSecs, 
                            retryDelaySecs=retryDelaySecs,
                            pollTimeoutSecs=pollTimeoutSecs,
                            noPoll=noPoll,
                            benchmarkLogging=benchmarkLogging)

                elapsed = time.time() - start
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
                    l = '{!s} jvms, {!s}GB heap, {:s} {:s} {:6.2f} MB/sec for {:.2f} secs'.format(
                        len(h2o.nodes), h2o.nodes[0].java_heap_GB, csvFilepattern, csvFilename, fileMBS, elapsed)
                    print l
                    h2o.cloudPerfH2O.message(l)

                print "Parse result['destination_key']:", parseResult['destination_key']

                # BUG here?
                if not noPoll:
                    # We should be able to see the parse result?
                    h2o_cmd.columnInfoFromInspect(parseResult['destination_key'], exceptionOnMissingValues=False)

                        
                # the nflx data doesn't have a small enough # of classes in any col
                # use exec to randomFilter out 200 rows for a quick RF. that should work for everyone?
                origKey = parseResult['destination_key']
                # execExpr = 'a = randomFilter('+origKey+',200,12345678)' 
                # execExpr = 'a = slice('+origKey+',1,200)' 
                execExpr = 'a = %s[1:200,]' % origKey
                h2e.exec_expr(h2o.nodes[0], execExpr, "a", timeoutSecs=30)
                # runRF takes the parseResult directly
                newParseKey = {'destination_key': 'a'}

                print "\n" + csvFilepattern
                # poker and the water.UDP.set3(UDP.java) fail issue..
                # constrain depth to 25
                print "Temporarily hacking to do nothing instead of RF on the parsed file"
                ### RFview = h2o_cmd.runRF(trees=1,depth=25,parseResult=newParseKey, timeoutSecs=timeoutSecs)
                ### h2b.browseJsonHistoryAsUrlLastMatch("RFView")

                #**********************************************************************************
                # Do GLM too
                # Argument case error: Value 0.0 is not between 12.0 and 9987.0 (inclusive)
                if DO_GLM:
                    # these are all the columns that are enums in the dataset...too many for GLM!
                    x = range(542) # don't include the output column
                    # remove the output too! (378)
                    for i in [3, 4, 5, 6, 7, 8, 9, 10, 11, 14, 16, 17, 18, 19, 20, 424, 425, 426, 540, 541, 378]:
                        x.remove(i)
                    x = ",".join(map(str,x))

                    GLMkwargs = {'x': x, 'y': 378, 'case': 15, 'case_mode': '>',
                        'max_iter': 10, 'n_folds': 1, 'alpha': 0.2, 'lambda': 1e-5}
                    start = time.time()
                    glm = h2o_cmd.runGLM(parseResult=parseResult, timeoutSecs=timeoutSecs, **GLMkwargs)
                    h2o_glm.simpleCheckGLM(self, glm, None, **GLMkwargs)
                    elapsed = time.time() - start
                    h2o.check_sandbox_for_errors()
                    l = '{:d} jvms, {:d}GB heap, {:s} {:s} GLM: {:6.2f} secs'.format(
                        len(h2o.nodes), h2o.nodes[0].java_heap_GB, csvFilepattern, csvFilename, elapsed)
                    print l
                    h2o.cloudPerfH2O.message(l)

                #**********************************************************************************

                h2o_cmd.checkKeyDistribution()
                h2i.delete_keys_from_import_result(pattern=csvFilename, importResult=importResult)

                ### time.sleep(3600)
                h2o.tear_down_cloud()
                if not h2o.localhost:
                    print "Waiting 30 secs before building cloud again (sticky ports?)"
                    ### time.sleep(30)

                sys.stdout.write('.')
                sys.stdout.flush() 

if __name__ == '__main__':
    h2o.unit_main()
