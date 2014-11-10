import unittest, sys, time
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_import as h2i, h2o_glm, h2o_common, h2o_exec as h2e
import h2o_print

DO_GLM = True
LOG_MACHINE_STATS = False

# fails during exec env push ..second import has to do a key delete (the first)
DO_DOUBLE_IMPORT = False

print "Assumes you ran ../build_for_clone.py in this directory"
print "Using h2o-nodes.json. Also the sandbox dir"
class releaseTest(h2o_common.ReleaseCommon, unittest.TestCase):

    def sub_c3_nongz_fvec_long(self, csvFilenameList):
        # a kludge
        h2o.setup_benchmark_log()

        bucket = 'home-0xdiag-datasets'
        importFolderPath = 'manyfiles-nflx'
        print "Using nongz'ed files in", importFolderPath

        if LOG_MACHINE_STATS:
            benchmarkLogging = ['cpu', 'disk', 'network']
        else:
            benchmarkLogging = []

        pollTimeoutSecs = 120
        retryDelaySecs = 10

        for trial, (csvFilepattern, csvFilename, totalBytes, timeoutSecs) in enumerate(csvFilenameList):
                csvPathname = importFolderPath + "/" + csvFilepattern

                if DO_DOUBLE_IMPORT:
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
                    hex_key="A.hex", timeoutSecs=timeoutSecs, 
                    retryDelaySecs=retryDelaySecs,
                    pollTimeoutSecs=pollTimeoutSecs,
                    benchmarkLogging=benchmarkLogging)
                elapsed = time.time() - start
                print "Parse #", trial, "completed in", "%6.2f" % elapsed, "seconds.", \
                    "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)

                print "Parse result['destination_key']:", parseResult['destination_key']
                h2o_cmd.columnInfoFromInspect(parseResult['destination_key'], exceptionOnMissingValues=False)

                fileMBS = (totalBytes/1e6)/elapsed
                msg = '{!s} jvms, {!s}GB heap, {:s} {:s} {:6.2f} MB/sec for {:.2f} secs'.format(
                    len(h2o.nodes), h2o.nodes[0].java_heap_GB, csvFilepattern, csvFilename, fileMBS, elapsed)
                print msg
                h2o.cloudPerfH2O.message(msg)
                h2o_cmd.checkKeyDistribution()

                # are the unparsed keys slowing down exec?
                h2i.delete_keys_at_all_nodes(pattern="manyfile")

                execExpr = 'B.hex=A.hex'
                h2e.exec_expr(execExpr=execExpr, timeoutSecs=180)
                h2o_cmd.checkKeyDistribution()

                execExpr = 'C.hex=B.hex'
                h2e.exec_expr(execExpr=execExpr, timeoutSecs=180)
                h2o_cmd.checkKeyDistribution()

                execExpr = 'D.hex=C.hex'
                h2e.exec_expr(execExpr=execExpr, timeoutSecs=180)
                h2o_cmd.checkKeyDistribution()

    #***********************************************************************
    # these will be tracked individual by jenkins, which is nice
    #***********************************************************************
    def test_c3_exec_copy(self):
        avgMichalSize = 237270000
        csvFilenameList= [
            ("*[1][0-4][0-9].dat", "file_50_A.dat", 50 * avgMichalSize, 1800),
        ]
        self.sub_c3_nongz_fvec_long(csvFilenameList)

if __name__ == '__main__':
    h2o.unit_main()
