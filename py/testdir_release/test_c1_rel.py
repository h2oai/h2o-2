import unittest, time, sys, random, logging
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_import2 as h2i, h2o_glm, h2o_common

print "Assumes you ran ../build_for_clone.py in this directory"
print "Creating a h2o-nodes.json to use. Also a sandbox dir!"
class releaseTest(h2o_common.ReleaseCommon, unittest.TestCase):

    def test_A_c1_rel_short(self):
        parseResult = h2i.import_parse(bucket='smalldata', path='iris/iris2.csv', schema='put')
        h2o_cmd.runRFOnly(parseResult=parseResult, trees=6, timeoutSecs=10)

    def test_B_c1_rel_long(self):
        # a kludge
        h2o.setup_benchmark_log()

        avgMichalSize = 116561140 
        bucket = 'home-0xdiag-datasets'
        importFolderPath = 'more1_1200_link'
        print "Using .gz'ed files in", importFolderPath
        csvFilenameList= [
            ("*[3][0][0-9].dat.gz", "file_10_A.dat.gz", 10 * avgMichalSize, 600),
        ]

        tryHeap = 28
        DO_GLM = False
        LOG_MACHINE_STATS = False

        if LOG_MACHINE_STATS:
            benchmarkLogging = ['cpu', 'disk', 'network']
        else:
            benchmarkLogging = []

        pollTimeoutSecs = 120
        retryDelaySecs = 10

        for trial, (csvFilepattern, csvFilename, totalBytes, timeoutSecs) in enumerate(csvFilenameList):
                csvPathname = importFolderPath + "/" + csvFilepattern

                (importResult, importPattern) = h2i.import_only(bucket=bucket, path=csvPathname, schema='local')
                importFullList = importResult['files']
                importFailList = importResult['fails']
                print "\n Problem if this is not empty: importFailList:", h2o.dump_json(importFailList)

                # this accumulates performance stats into a benchmark log over multiple runs 
                # good for tracking whether we're getting slower or faster
                h2o.cloudPerfH2O.change_logfile(csvFilename)
                h2o.cloudPerfH2O.message("")
                h2o.cloudPerfH2O.message("Parse " + csvFilename + " Start--------------------------------")

                start = time.time()
                parseResult = h2i.import_parse(bucket=bucket, path=csvPathname, schema='local',
                    hex_key=csvFilename + ".hex", timeoutSecs=timeoutSecs, 
                    retryDelaySecs=retryDelaySecs,
                    pollTimeoutSecs=pollTimeoutSecs,
                    benchmarkLogging=benchmarkLogging)
                elapsed = time.time() - start
                print "Parse #", trial, "completed in", "%6.2f" % elapsed, "seconds.", \
                    "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)

                print csvFilepattern, 'parse time:', parseResult['response']['time']
                print "Parse result['destination_key']:", parseResult['destination_key']
                h2o_cmd.columnInfoFromInspect(parseResult['destination_key'], exceptionOnMissingValues=False)

                if totalBytes is not None:
                    fileMBS = (totalBytes/1e6)/elapsed
                    msg = '{!s} jvms, {!s}GB heap, {:s} {:s} {:6.2f} MB/sec for {:.2f} secs'.format(
                        len(h2o.nodes), h2o.nodes[0].java_heap_GB, csvFilepattern, csvFilename, fileMBS, elapsed)
                    print msg
                    h2o.cloudPerfH2O.message(msg)

                if DO_GLM:
                    # these are all the columns that are enums in the dataset...too many for GLM!
                    x = range(542) # don't include the output column
                    # remove the output too! (378)
                    for i in [3,4,5,6,7,8,9,10,11,14,16,17,18,19,20,424,425,426,540,541,378]:
                        x.remove(i)
                    x = ",".join(map(str,x))

                    GLMkwargs = {'x': x, 'y': 378, 'case': 15, 'case_mode': '>',
                        'max_iter': 10, 'n_folds': 1, 'alpha': 0.2, 'lambda': 1e-5}

                    start = time.time()
                    glm = h2o_cmd.runGLMOnly(parseResult=parseResult, timeoutSecs=timeoutSecs, **GLMkwargs)
                    elapsed = time.time() - start
                    h2o.check_sandbox_for_errors()

                    h2o_glm.simpleCheckGLM(self, glm, None, **GLMkwargs)
                    msg = '{:d} jvms, {:d}GB heap, {:s} {:s} GLM: {:6.2f} secs'.format(
                        len(h2o.nodes), h2o.nodes[0].java_heap_GB, csvFilepattern, csvFilename, elapsed)
                    print msg
                    h2o.cloudPerfH2O.message(msg)

                h2o_cmd.checkKeyDistribution()

if __name__ == '__main__':
    h2o.unit_main()
