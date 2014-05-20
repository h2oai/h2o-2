import unittest, sys, time
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_import as h2i, h2o_glm, h2o_common, h2o_exec as h2e, h2o_hosts
import h2o_print

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED, localhost
        SEED = h2o.setup_random_seed()
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(2, java_heap_GB=6)
        else:
            h2o_hosts.build_cloud_with_hosts(2, java_heap_GB=6)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_50_nongz_fvec(self):
        h2o.beta_features = True
        avgMichalSize = 237270000
        bucket = 'home-0xdiag-datasets'
        importFolderPath = 'manyfiles-nflx'
        importFolderPath = 'airlines'
        print "Using non-gz'ed files in", importFolderPath
        csvFilenameList= [
            ("*[1][0][0].dat", "file_1_A.dat", 1 * avgMichalSize, 1800),
            # ("*[1][0-4][0-9].dat", "file_50_A.dat", 50 * avgMichalSize, 1800),
            # ("*[1][0-4][0-9].dat", "file_50_A.dat", 50 * avgMichalSize, 1800),
            # ("*[1][0-4][0-9].dat", "file_50_A.dat", 50 * avgMichalSize, 1800),
        ]

        pollTimeoutSecs = 120
        retryDelaySecs = 10

        for trial, (csvFilepattern, csvFilename, totalBytes, timeoutSecs) in enumerate(csvFilenameList):
            csvPathname = importFolderPath + "/" + csvFilepattern

            (importResult, importPattern) = h2i.import_only(bucket=bucket, path=csvPathname, schema='local')
            importFullList = importResult['files']
            importFailList = importResult['fails']
            print "\n Problem if this is not empty: importFailList:", h2o.dump_json(importFailList)


            (importResult, importPattern) = h2i.import_only(bucket=bucket, path=csvPathname, schema='local')
            importFullList = importResult['files']
            importFailList = importResult['fails']
            print "\n Problem if this is not empty: importFailList:", h2o.dump_json(importFailList)

            h2o_cmd.runStoreView(timeoutSecs=60)

if __name__ == '__main__':
    h2o.unit_main()
