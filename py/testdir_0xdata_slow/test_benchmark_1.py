import os, json, unittest, time, shutil, sys
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd,h2o_hosts, h2o_browse as h2b, h2o_import as h2i, h2o_hosts, h2o_glm
import h2o_exec as h2e, h2o_jobs
import time, random, logging

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
            importFolderPath = '/home/0xdiag/datasets/more1_1200_link'
            print "Using .gz'ed files in", importFolderPath
            csvFilenameAll = [
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
                ("*[3-6][0-9][0-9].dat.gz", "file_400.dat.gz", 400 * avgMichalSize, 3600),
                ("*[3-6][0-9][0-9].dat.gz", "file_400.dat.gz", 400 * avgMichalSize, 3600),
                ("*[3-6][0-9][0-9].dat.gz", "file_400.dat.gz", 400 * avgMichalSize, 3600),
                ("*[3-6][0-9][0-9].dat.gz", "file_400.dat.gz", 400 * avgMichalSize, 3600),
                ("*[3-6][0-9][0-9].dat.gz", "file_400.dat.gz", 400 * avgMichalSize, 3600),
                ("*[3-6][0-9][0-9].dat.gz", "file_400.dat.gz", 400 * avgMichalSize, 3600),
                ("*[3-6][0-9][0-9].dat.gz", "file_400.dat.gz", 400 * avgMichalSize, 3600),
                ("*[3-6][0-9][0-9].dat.gz", "file_400.dat.gz", 400 * avgMichalSize, 3600),
            ]

        # csvFilenameList = random.sample(csvFilenameAll,1)
        csvFilenameList = csvFilenameAll

        # split out the pattern match and the filename used for the hex
        trialMax = 1
        # rebuild the cloud for each file
        base_port = 54321
        tryHeap = 28
        # can fire a parse off and go wait on the jobs queue (inspect afterwards is enough?)
        DO_GLM = False
        noPoll = False
        # benchmarkLogging = ['cpu','disk', 'iostats', 'jstack']
        # benchmarkLogging = None
        benchmarkLogging = ['cpu','disk', 'network', 'iostats', 'jstack']
        benchmarkLogging = ['cpu','disk', 'network', 'iostats']
        # IOStatus can hang?
        benchmarkLogging = ['cpu', 'disk' 'network']
        pollTimeoutSecs = 120
        retryDelaySecs = 10

        for i,(csvFilepattern, csvFilename, totalBytes, timeoutSecs) in enumerate(csvFilenameList):
            localhost = h2o.decide_if_localhost()
            if (localhost):
                h2o.build_cloud(2,java_heap_GB=tryHeap, base_port=base_port,
                    enable_benchmark_log=True)

            else:
                h2o_hosts.build_cloud_with_hosts(1, java_heap_GB=tryHeap, base_port=base_port, 
                    enable_benchmark_log=True)

            # pop open a browser on the cloud
            ### h2b.browseTheCloud()

            # to avoid sticky ports?
            ### base_port += 2

            for trial in range(trialMax):
                importFolderResult = h2i.setupImportFolder(None, importFolderPath)
                importFullList = importFolderResult['files']
                importFailList = importFolderResult['fails']
                print "\n Problem if this is not empty: importFailList:", h2o.dump_json(importFailList)
                # creates csvFilename.hex from file in importFolder dir 

                h2o.cloudPerfH2O.change_logfile(csvFilename)
                h2o.cloudPerfH2O.message("")
                h2o.cloudPerfH2O.message("Parse " + csvFilename + " Start--------------------------------")
                start = time.time()
                parseKey = h2i.parseImportFolderFile(None, csvFilepattern, importFolderPath, 
                    key2=csvFilename + ".hex", timeoutSecs=timeoutSecs, 
                    retryDelaySecs=retryDelaySecs,
                    pollTimeoutSecs=pollTimeoutSecs,
                    noPoll=noPoll,
                    benchmarkLogging=benchmarkLogging)

                if noPoll:
                    if (i+1) < len(csvFilenameList):
                        time.sleep(1)
                        h2o.check_sandbox_for_errors()
                        (csvFilepattern, csvFilename, totalBytes2, timeoutSecs) = csvFilenameList[i+1]
                        parseKey = h2i.parseImportFolderFile(None, csvFilepattern, importFolderPath, 
                            key2=csvFilename + ".hex", timeoutSecs=timeoutSecs, 
                            retryDelaySecs=retryDelaySecs,
                            pollTimeoutSecs=pollTimeoutSecs,
                            noPoll=noPoll,
                            benchmarkLogging=benchmarkLogging)

                    if (i+2) < len(csvFilenameList):
                        time.sleep(1)
                        h2o.check_sandbox_for_errors()
                        (csvFilepattern, csvFilename, totalBytes3, timeoutSecs) = csvFilenameList[i+2]
                        parseKey = h2i.parseImportFolderFile(None, csvFilepattern, importFolderPath, 
                            key2=csvFilename + ".hex", timeoutSecs=timeoutSecs, 
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
                        len(h2o.nodes), tryHeap, csvFilepattern, csvFilename, fileMBS, elapsed)
                    print l
                    h2o.cloudPerfH2O.message(l)

                print csvFilepattern, 'parse time:', parseKey['response']['time']
                print "Parse result['destination_key']:", parseKey['destination_key']

                # BUG here?
                if not noPoll:
                    # We should be able to see the parse result?
                    h2o_cmd.check_enums_from_inspect(parseKey)
                        
                # the nflx data doesn't have a small enough # of classes in any col
                # use exec to randomFilter out 200 rows for a quick RF. that should work for everyone?
                origKey = parseKey['destination_key']
                # execExpr = 'a = randomFilter('+origKey+',200,12345678)' 
                execExpr = 'a = slice('+origKey+',1,200)' 
                h2e.exec_expr(h2o.nodes[0], execExpr, "a", timeoutSecs=30)
                # runRFOnly takes the parseKey directly
                newParseKey = {'destination_key': 'a'}

                print "\n" + csvFilepattern
                # poker and the water.UDP.set3(UDP.java) fail issue..
                # constrain depth to 25
                print "Temporarily hacking to do nothing instead of RF on the parsed file"
                ### RFview = h2o_cmd.runRFOnly(trees=1,depth=25,parseKey=newParseKey, timeoutSecs=timeoutSecs)
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
                    glm = h2o_cmd.runGLMOnly(parseKey=parseKey, timeoutSecs=timeoutSecs, **GLMkwargs)
                    h2o_glm.simpleCheckGLM(self, glm, None, **GLMkwargs)
                    elapsed = time.time() - start
                    h2o.check_sandbox_for_errors()
                    l = '{:d} jvms, {:d}GB heap, {:s} {:s} GLM: {:6.2f} secs'.format(
                        len(h2o.nodes), tryHeap, csvFilepattern, csvFilename, elapsed)
                    print l
                    h2o.cloudPerfH2O.message(l)

                #**********************************************************************************
                h2o_cmd.check_key_distribution()
                h2o_cmd.delete_csv_key(csvFilename, importFullList)
                ### time.sleep(3600)
                h2o.tear_down_cloud()
                if not localhost:
                    print "Waiting 30 secs before building cloud again (sticky ports?)"
                    ### time.sleep(30)

                sys.stdout.write('.')
                sys.stdout.flush() 

if __name__ == '__main__':
    h2o.unit_main()
