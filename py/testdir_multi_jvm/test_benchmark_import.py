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
        if 1==0:
            # importFolderPath = '/home/0xdiag/datasets'
            importFolderPath = '/home/0xdiag/datasets'
            print "Using .gz'ed files in", importFolderPath
            csvFilenameAll = [
                # this should hit the "more" files too?
                ("10k_small_gz/*", "file_400.dat.gz", 10000 * synSize , 700),
            ]

        if 1==1:
            importFolderPath = '/home/0xdiag/datasets/more1_1200_link'
            print "Using .gz'ed files in", importFolderPath
            csvFilenameAll = [
                # this should hit the "more" files too?
                # ("*.dat.gz", "file_200.dat.gz", 1200 * avgMichalSize, 1800),
                # ("*.dat.gz", "file_200.dat.gz", 1200 * avgMichalSize, 1800),
                # ("*[1][0-2][0-9].dat.gz", "file_30.dat.gz", 50 * avgMichalSize, 1800), 
                ("*[1][0-3][0-9].dat.gz", "file_40.dat.gz", 50 * avgMichalSize, 1800), 
                ("*[1][0-4][0-9].dat.gz", "file_50.dat.gz", 50 * avgMichalSize, 1800), 
                # ("*[1][0-9][0-9].dat.gz", "file_100.dat.gz", 100 * avgMichalSize, 1800), 
                # ("*[12][0-9][0-9].dat.gz", "file_200.dat.gz", 200 * avgMichalSize, 1800), 
            ]

        if 1==0:
            importFolderPath = '/home/0xdiag/datasets/more1_1200_link'
            print "Using .gz'ed files in", importFolderPath
            csvFilenameAll = [
                # this should hit the "more" files too?
                # ("*[1-8][0-9][0-9].dat.gz", "file_800.dat.gz", 800 * avgMichalSize, 1800),

                # ("*[1-4][0-9][0-9].dat.gz", "file_400.dat.gz", 400 * avgMichalSize, 1800), # fails tcp reset
                # ("*[1-2][0-9][0-9].dat.gz", "file_200.dat.gz", 200 * avgMichalSize, 1800),  # fails tcp
                # ("*[1][0-9][0-9].dat.gz", "file_100.dat.gz", 100 * avgMichalSize, 1800),  # fails tcp
                # ("*[1][0-9][0-9].dat.gz", "file_100.dat.gz", 100 * avgMichalSize, 1800),

                # ("*10[0-9].dat.gz", "file_10.dat.gz", 10 * avgMichalSize, 1800), 
                # ("*1[0-1][0-9].dat.gz", "file_20_2jvm.dat.gz", 20 * avgMichalSize, 1800), 
                # ("*1[0-4][0-9].dat.gz", "file_50_2jvm.dat.gz", 50 * avgMichalSize, 1800), 
                # ("*10[0-9].dat.gz", "file_10_2jvm.dat.gz", 10 * avgMichalSize, 1800), 
                # ("*1[0-4][0-9].dat.gz", "file_50.dat.gz", 50 * avgMichalSize, 1800), 
                ("*1[0-9][0-9].dat.gz", "file_100.dat.gz", 100 * avgMichalSize, 1800), 
            ]

        if 1==0:
            importFolderPath = '/home/0xdiag/datasets/more1_300_link'
            print "Using .gz'ed files in", importFolderPath
            csvFilenameAll = [
                # this should hit the "more" files too?
                ("*.dat.gz", "file_300.dat.gz", 300 * avgMichalSize, 1800),
            ]

        if 1==0:
            importFolderPath = '/home/0xdiag/datasets/manyfiles-nflx-gz'
            print "Using .gz'ed files in", importFolderPath
            csvFilenameAll = [
                # this should hit the "more" files too?
                ("*_[123][0-9][0-9]*.dat.gz", "file_600.dat.gz", 2 * 300 * avgMichalSize, 1800),
                ("*_[1][5-9][0-9]*.dat.gz", "file_100.dat.gz", 2 * 50 * avgMichalSize, 1800),
            ]

        if 1==0:
            importFolderPath = '/home2/0xdiag/datasets'
            print "Using non-.gz'ed files in", importFolderPath
            csvFilenameAll = [
                # I use different files to avoid OS caching effects
                ("manyfiles-nflx/file_[0-9][0-9]*.dat", "file_100.dat", 100 * avgMichalSizeUncompressed, 700),
                ("manyfiles-nflx/file_[0-9][0-9]*.dat", "file_100.dat", 100 * avgMichalSizeUncompressed, 700),
                ("manyfiles-nflx/file_[0-9][0-9]*.dat", "file_100.dat", 100 * avgMichalSizeUncompressed, 700),
                # ("onefile-nflx/file_1_to_100.dat", "file_single.dat", 100 * avgMichalSizeUncompressed, 1200),
                # ("manyfiles-nflx/file_1.dat", "file_1.dat", 1 * avgMichalSizeUncompressed, 700),
                # ("manyfiles-nflx/file_[2][0-9].dat", "file_10.dat", 10 * avgMichalSizeUncompressed, 700),
                # ("manyfiles-nflx/file_[34][0-9].dat", "file_20.dat", 20 * avgMichalSizeUncompressed, 700),
                # ("manyfiles-nflx/file_[5-9][0-9].dat", "file_50.dat", 50 * avgMichalSizeUncompressed, 700),
            ]
        if 1==0: 
            importFolderPath = '/home/0xdiag/datasets'
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

                ("manyfiles-nflx-gz/file_*.dat.gz", "file_100.dat.gz", 100 * avgMichalSize, 1200),
                ("manyfiles-nflx-gz/file_1.dat.gz", "file_1.dat.gz", 1 * avgMichalSize, 700),
                ("manyfiles-nflx-gz/file_[2][0-9].dat.gz", "file_10.dat.gz", 10 * avgMichalSize, 700),
                ("manyfiles-nflx-gz/file_[34][0-9].dat.gz", "file_20.dat.gz", 20 * avgMichalSize, 700),
                ("manyfiles-nflx-gz/file_[5-9][0-9].dat.gz", "file_50.dat.gz", 50 * avgMichalSize, 700),
                # ("covtype200x.data", "covtype200x.data", covtype200xSize, 700),

                # do it twice
                # ("covtype.data", "covtype.data"),
                # ("covtype20x.data", "covtype20x.data"),
                # "covtype200x.data",
                # "100million_rows.csv",
                # "200million_rows.csv",
                # "a5m.csv",
                # "a10m.csv",
                # "a100m.csv",
                # "a200m.csv",
                # "a400m.csv",
                # "a600m.csv",
                # "billion_rows.csv.gz",
                # "new-poker-hand.full.311M.txt.gz",
                ]
        # csvFilenameList = random.sample(csvFilenameAll,1)
        csvFilenameList = csvFilenameAll

        # split out the pattern match and the filename used for the hex
        trialMax = 1
        # rebuild the cloud for each file
        base_port = 54321
        tryHeap = 15
        # can fire a parse off and go wait on the jobs queue (inspect afterwards is enough?)
        DO_GLM = False
        noPoll = False
        # benchmarkLogging = ['cpu','disk', 'iostats', 'jstack']
        # benchmarkLogging = None
        benchmarkLogging = ['cpu','disk', 'network', 'iostats']
        pollTimeoutSecs = 120
        retryDelaySecs = 10

        jea = '-XX:MaxDirectMemorySize=512m -XX:+PrintGCDetails' + ' -Dh2o.find-ByteBuffer-leaks'
        jea = '-XX:MaxDirectMemorySize=512m -XX:+PrintGCDetails'
        jea = ' -Dcom.sun.management.jmxremote.port=54330' + \
              ' -Dcom.sun.management.jmxremote.authenticate=false' + \
              ' -Dcom.sun.management.jmxremote.ssl=false'  + \
              ' -Dcom.sun.management.jmxremote' + \
              ' -Dcom.sun.management.jmxremote.local.only=false'

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
                importFullList = importFolderResult['succeeded']
                importFailList = importFolderResult['failed']
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
