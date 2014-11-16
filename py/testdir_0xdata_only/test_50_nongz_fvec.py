import unittest, sys, time
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_import as h2i, h2o_glm, h2o_common, h2o_exec as h2e
import h2o_print

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = h2o.setup_random_seed()
        h2o.init(2, java_heap_GB=40)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_50_nongz_fvec(self):
        avgMichalSize = 237270000
        bucket = 'home-0xdiag-datasets'
        importFolderPath = 'manyfiles-nflx'
        print "Using non-gz'ed files in", importFolderPath
        csvFilenameList= [
            ("*[1][0-4][0-9].dat", "file_50_A.dat", 50 * avgMichalSize, 1800),
        ]

        pollTimeoutSecs = 120
        retryDelaySecs = 10

        for trial, (csvFilepattern, csvFilename, totalBytes, timeoutSecs) in enumerate(csvFilenameList):
            csvPathname = importFolderPath + "/" + csvFilepattern

            (importResult, importPattern) = h2i.import_only(bucket=bucket, path=csvPathname, schema='local')
            importFullList = importResult['files']
            importFailList = importResult['fails']
            print "\n Problem if this is not empty: importFailList:", h2o.dump_json(importFailList)

            start = time.time()
            parseResult = h2i.import_parse(bucket=bucket, path=csvPathname, schema='local',
                hex_key=csvFilename + ".hex", timeoutSecs=timeoutSecs, 
                retryDelaySecs=retryDelaySecs,
                pollTimeoutSecs=pollTimeoutSecs)
            elapsed = time.time() - start
            print "Parse #", trial, "completed in", "%6.2f" % elapsed, "seconds.", \
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)

            print "Parse result['destination_key']:", parseResult['destination_key']
            h2o_cmd.columnInfoFromInspect(parseResult['destination_key'], exceptionOnMissingValues=False)

            if totalBytes is not None:
                fileMBS = (totalBytes/1e6)/elapsed
                msg = '{!s} jvms, {!s}GB heap, {:s} {:s} {:6.2f} MB/sec for {:.2f} secs'.format(
                    len(h2o.nodes), h2o.nodes[0].java_heap_GB, csvFilepattern, csvFilename, fileMBS, elapsed)
                print msg


if __name__ == '__main__':
    h2o.unit_main()
