import unittest, time, sys, random, logging
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i, h2o_glm
import h2o_exec as h2e, h2o_jobs

DO_IMPORT_CHECK = True
class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        pass

    @classmethod
    def tearDownClass(cls):
        # time.sleep(3600)
        h2o.tear_down_cloud()

    def test_benchmark_import(self):
        # typical size of the michal files
        avgMichalSizeUncompressed = 237270000 
        avgMichalSize = 116561140 
        avgSynSize = 4020000
        covtype200xSize = 15033863400
        synSize =  183
        if 1==1:
            # importFolderPath = '/home/0xdiag/datasets/more1_1200_link'
            # importFolderPathFull = '/home/0xdiag/datasets/manyfiles-nflx-gz'
            # importFolderPath = 'more1_1200_link'
            importFolderPath = 'manyfiles-nflx-gz'
            print "Using .gz'ed files in", importFolderPath
            # this pattern from browser correctly does 100 files, 1M rowsj
            # source_key=*/home/0xdiag/datasets/manyfiles-nflx-gz/file_1[0-9][0-9].dat.gz
            csvFilenameAll = [
                ("file_1.dat.gz", "file_1_A.dat.gz", 1 * avgMichalSize, 3600),
                ("*[3-4][0-4][0-9].dat.gz", "file_100_A.dat.gz", 100 * avgMichalSize, 3600),
                ("*[3-4][0-4][0-9].dat.gz", "file_100_B.dat.gz", 100 * avgMichalSize, 3600),

                # ("*[3-4][0-5][0-9].dat.gz", "file_120_A.dat.gz", 120 * avgMichalSize, 3600),
                # ("*[3-4][0-5][0-9].dat.gz", "file_120_B.dat.gz", 120 * avgMichalSize, 3600),

                ("*[3-4][0-6][0-9].dat.gz", "file_140_A.dat.gz", 140 * avgMichalSize, 3600),
                ("*[3-4][0-6][0-9].dat.gz", "file_140_B.dat.gz", 140 * avgMichalSize, 3600),

                # ("*[3-4][0-7][0-9].dat.gz", "file_160_A.dat.gz", 160 * avgMichalSize, 3600),
                # ("*[3-4][0-7][0-9].dat.gz", "file_160_B.dat.gz", 160 * avgMichalSize, 3600),

                ("*[3-4][0-8][0-9].dat.gz", "file_180_A.dat.gz", 180 * avgMichalSize, 3600),
                ("*[3-4][0-8][0-9].dat.gz", "file_180_B.dat.gz", 180 * avgMichalSize, 3600),

                ("*[3-4][0-9][0-9].dat.gz", "file_200_A.dat.gz", 200 * avgMichalSize, 3600),
                ("*[3-4][0-9][0-9].dat.gz", "file_200_B.dat.gz", 200 * avgMichalSize, 3600),

                ("*[3-5][0-9][0-9].dat.gz", "file_300.dat.gz", 300 * avgMichalSize, 3600),
                ("*[3-5][0-9][0-9].dat.gz", "file_300.dat.gz", 300 * avgMichalSize, 3600),

                # ("*[3-6][0-9][0-9].dat.gz", "file_400.dat.gz", 400 * avgMichalSize, 3600),
                # ("*[3-6][0-9][0-9].dat.gz", "file_400.dat.gz", 400 * avgMichalSize, 3600),
                # ("*[3-6][0-9][0-9].dat.gz", "file_400.dat.gz", 400 * avgMichalSize, 3600),
                # ("*[3-6][0-9][0-9].dat.gz", "file_400.dat.gz", 400 * avgMichalSize, 3600),
                # ("*[3-6][0-9][0-9].dat.gz", "file_400.dat.gz", 400 * avgMichalSize, 3600),
                # ("*[3-6][0-9][0-9].dat.gz", "file_400.dat.gz", 400 * avgMichalSize, 3600),
                # ("*[3-6][0-9][0-9].dat.gz", "file_400.dat.gz", 400 * avgMichalSize, 3600),
                # ("*[3-6][0-9][0-9].dat.gz", "file_400.dat.gz", 400 * avgMichalSize, 3600),
                # ("*[3-6][0-9][0-9].dat.gz", "file_400.dat.gz", 400 * avgMichalSize, 3600),
            ]

        # csvFilenameList = random.sample(csvFilenameAll,1)
        csvFilenameList = csvFilenameAll

        # split out the pattern match and the filename used for the hex
        trialMax = 1
        # rebuild the cloud for each file
        # can fire a parse off and go wait on the jobs queue (inspect afterwards is enough?)
        DO_GLM = False
        noPoll = False
        # benchmarkLogging = ['cpu','disk', 'iostats', 'jstack']
        # benchmarkLogging = None
        benchmarkLogging = ['cpu','disk', 'network', 'iostats', 'jstack']
        benchmarkLogging = ['cpu','disk', 'network', 'iostats']
        # IOStatus can hang?
        benchmarkLogging = ['cpu', 'disk' 'network']
        pollTimeoutSecs = 180
        retryDelaySecs = 10

            tryHeap = 4
        h2o.init(2,java_heap_GB=tryHeap, enable_benchmark_log=True)
            tryHeap = 28

        for i,(csvFilepattern, csvFilename, totalBytes, timeoutSecs) in enumerate(csvFilenameList):
            # pop open a browser on the cloud
            ### h2b.browseTheCloud()

            # to avoid sticky ports?

            for trial in range(trialMax):
                # (importResult, importPattern) = h2i.import_only(path=importFolderPath+"/*")

                if DO_IMPORT_CHECK:
                    for i in range(2):
                        csvPathname = importFolderPath + "/" + csvFilepattern
                        (importResult, importPattern) = h2i.import_only(bucket='home-0xdiag-datasets', 
                                path=csvPathname, schema='local', timeoutSecs=timeoutSecs)

                        importFullList = importResult['files']
                        importFailList = importResult['fails']
                        print "\n Problem if this is not empty: importFailList:", h2o.dump_json(importFailList)
                        # creates csvFilename.hex from file in importFolder dir 

                h2o.cloudPerfH2O.change_logfile(csvFilename)
                h2o.cloudPerfH2O.message("")
                h2o.cloudPerfH2O.message("Parse " + csvFilename + " Start--------------------------------")
                csvPathname = importFolderPath + "/" + csvFilepattern
                start = time.time()
                parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathname, schema='local',
                    hex_key=csvFilename + ".hex", timeoutSecs=timeoutSecs, 
                    retryDelaySecs=retryDelaySecs,
                    pollTimeoutSecs=pollTimeoutSecs,
                    noPoll=noPoll,
                    benchmarkLogging=benchmarkLogging)
                elapsed = time.time() - start
                print "Parse#", trial, parseResult['destination_key'], "took", elapsed, "seconds",\
                    "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)

                inspect = h2o_cmd.runInspect(None, parseResult['destination_key'], timeoutSecs=360)
                h2o_cmd.infoFromInspect(inspect, csvPathname)

                if noPoll:
                    if (i+1) < len(csvFilenameList):
                        h2o.check_sandbox_for_errors()
                        (csvFilepattern, csvFilename, totalBytes2, timeoutSecs) = csvFilenameList[i+1]
                        # parseResult = h2i.import_parse(path=importFolderPath + "/" + csvFilepattern,
                        csvPathname = importFolderPathFull + "/" + csvFilepattern
                        start = time.time()
                        parseResult = h2i.import_parse(path=csvPathname,
                            hex_key=csvFilename + ".hex", 
                            timeoutSecs=timeoutSecs, 
                            retryDelaySecs=retryDelaySecs,
                            pollTimeoutSecs=pollTimeoutSecs,
                            noPoll=noPoll,
                            benchmarkLogging=benchmarkLogging)
                        elapsed = time.time() - start
                        print "Parse#", trial, parseResult['destination_key'], "took", elapsed, "seconds",\
                            "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)
                        inspect = h2o_cmd.runInspect(None, parseResult['destination_key'], timeoutSecs=360)
                        h2o_cmd.infoFromInspect(inspect, csvPathname)

                    if (i+2) < len(csvFilenameList):
                        h2o.check_sandbox_for_errors()
                        (csvFilepattern, csvFilename, totalBytes3, timeoutSecs) = csvFilenameList[i+2]
                        csvPathname = importFolderPathFull + "/" + csvFilepattern
                        parseResult = h2i.import_parse(path=csvPathname,
                            hex_key=csvFilename + ".hex", timeoutSecs=timeoutSecs, 
                            retryDelaySecs=retryDelaySecs,
                            pollTimeoutSecs=pollTimeoutSecs,
                            noPoll=noPoll,
                            benchmarkLogging=benchmarkLogging)
                        elapsed = time.time() - start
                        print "Parse#", trial, parseResult['destination_key'], "took", elapsed, "seconds",\
                            "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)
                        inspect = h2o_cmd.runInspect(None, parseResult['destination_key'], timeoutSecs=360)
                        h2o_cmd.infoFromInspect(inspect, csvPathname)


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
                        len(h2o.nodes), tryHeap, csvFilepattern, csvFilename, fileMBS, elapsed)
                    print l
                    h2o.cloudPerfH2O.message(l)

                print "Parse result['destination_key']:", parseResult['destination_key']

                # BUG here?
                if not noPoll:
                    pass
                    # We should be able to see the parse result?
                    # h2o_cmd.check_enums_from_inspect(parseResult)
                        
                # the nflx data doesn't have a small enough # of classes in any col
                # use exec to randomFilter out 200 rows for a quick RF. that should work for everyone?
                origKey = parseResult['destination_key']
                # execExpr = 'a = randomFilter('+origKey+',200,12345678)' 
                execExpr = 'a = slice('+origKey+',1,200)' 
                # h2e.exec_expr(h2o.nodes[0], execExpr, "a", timeoutSecs=30)
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
                        len(h2o.nodes), tryHeap, csvFilepattern, csvFilename, elapsed)
                    print l
                    h2o.cloudPerfH2O.message(l)

                #**********************************************************************************
                # print "Waiting 30 secs"
                # time.sleep(30)

                h2o_cmd.checkKeyDistribution()
                h2i.delete_keys_from_import_result(pattern=csvFilename, importResult=importResult)
                h2o.nodes[0].remove_all_keys()

                ### time.sleep(3600)

                ### h2o.tear_down_cloud()
                if not h2o.localhost:
                    print "Waiting 30 secs before building cloud again (sticky ports?)"
                    ### time.sleep(30)

                sys.stdout.write('.')
                sys.stdout.flush() 

if __name__ == '__main__':
    h2o.unit_main()
