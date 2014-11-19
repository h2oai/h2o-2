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
        covtype200xSize = 15033863400

        csvFilenameList = [
            ("covtype200x.data", "covtype200x.data", covtype200xSize, 700),
            ]

        trialMax = 1
        tryHeap = 28
        # can fire a parse off and go wait on the jobs queue (inspect afterwards is enough?)
        DO_GLM = False
        noPoll = False
        benchmarkLogging = ['cpu', 'disk' 'network']
        pollTimeoutSecs = 120
        retryDelaySecs = 10
        bucket = 'home-0xdiag-datasets'
        importFolderPath = 'standard'

        for i,(csvFilepattern, csvFilename, totalBytes, timeoutSecs) in enumerate(csvFilenameList):
            h2o.init(2,java_heap_GB=tryHeap, enable_benchmark_log=True)

            for trial in range(trialMax):
                csvPathname = importFolderPath + "/" + csvFilepattern

                h2o.cloudPerfH2O.change_logfile(csvFilename)
                h2o.cloudPerfH2O.message("")
                h2o.cloudPerfH2O.message("Parse " + csvFilename + " Start--------------------------------")

                start = time.time()
                parseResult = h2i.import_parse(bucket=bucket, path=csvPathname, schema='local', hex_key=csvFilename + ".hex", 
                    timeoutSecs=timeoutSecs, 
                    retryDelaySecs=retryDelaySecs,
                    pollTimeoutSecs=pollTimeoutSecs,
                    noPoll=noPoll,
                    benchmarkLogging=benchmarkLogging)

                elapsed = time.time() - start
                print "Parse #", trial, "completed in", "%6.2f" % elapsed, "seconds.", \
                    "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)

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
                    # We should be able to see the parse result?
                    h2o_cmd.check_enums_from_inspect(parseResult)
                        
                # use exec to randomFilter out 200 rows for a quick RF. that should work for everyone?
                origKey = parseResult['destination_key']
                # execExpr = 'a = randomFilter('+origKey+',200,12345678)' 
                execExpr = 'a = slice('+origKey+',1,200)' 
                h2e.exec_expr(h2o.nodes[0], execExpr, "a", timeoutSecs=30)
                # runRF takes the parseResult directly
                newParseKey = {'destination_key': 'a'}

                print "\n" + csvFilepattern

                #**********************************************************************************
                if DO_GLM:
                    # these are all the columns that are enums in the dataset...too many for GLM!
                    x = range(54) # don't include the output column
                    x = ",".join(map(str,x))

                    GLMkwargs = {'x': x, 'y': 54, 'case': 1, 'case_mode': '>',
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
                h2o_cmd.checkKeyDistribution()
                h2o.tear_down_cloud()

                sys.stdout.write('.')
                sys.stdout.flush() 

if __name__ == '__main__':
    h2o.unit_main()
