import unittest, time, sys, time, random, json
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i, h2o_common, h2o_jobs as h2j

DO_RANDOM_SAMPLE = True
DO_RF = False
print "Assumes you ran ../build_for_clone.py in this directory"
print "Using h2o-nodes.json. Also the sandbox dir"

class releaseTest(h2o_common.ReleaseCommon, unittest.TestCase):

    def test_c6_maprfs_fvec(self):
        print "\nLoad a list of files from maprfs, parse and do 1 RF tree"
        # larger set in my local dir
        # fails because classes aren't integers
        #    "allstate_claim_prediction_train_set.zip",
        csvFilenameAll = [
            # "3G_poker_shuffle",
            "TEST-poker1000.csv",
            "and-testing.data",
            "arcene2_train.both",
            "arcene_train.both",
            "bestbuy_test.csv",
            "bestbuy_train.csv",
            # "billion_rows.csv.gz",
            "covtype.13x.data",
            "covtype.13x.shuffle.data",
            # "covtype.169x.data",
            "covtype.4x.shuffle.data",
            "covtype.data",
            "covtype4x.shuffle.data",
            "hhp.unbalanced.012.1x11.data.gz",
            "hhp.unbalanced.012.data.gz",
            "hhp.unbalanced.data.gz",
            # duplicate column header "A"
            # "hhp2.os.noisy.0_1.data",
            # "hhp2.os.noisy.9_4.data",
            "hhp_9_14_12.data",
            "leads.csv",
            "prostate_long_1G.csv",
        ]

        # find_cloud.py won't set these correctly. Let's just set them here
        # h2o.nodes[0].use_maprfs = True
        # h2o.nodes[0].use_hdfs = False
        # h2o.nodes[0].hdfs_version = 'mapr3.0.1',
        # h2o.nodes[0].hdfs_name_node = '172.16.2.171:7222'

        h2o.setup_benchmark_log()

        # benchmarkLogging = ['cpu','disk', 'network', 'iostats', 'jstack']
        # benchmarkLogging = ['cpu','disk', 'network', 'iostats']
        # benchmarkLogging = ['cpu', 'disk', 'network']
        benchmarkLogging = []

        # pick 8 randomly!
        if DO_RANDOM_SAMPLE:
            csvFilenameList = random.sample(csvFilenameAll,8)
        # Alternatively: do the list in order! Note the order is easy to hard
        else:
            csvFilenameList = csvFilenameAll

        # save the first, for all comparisions, to avoid slow drift with each iteration
        importFolderPath = "datasets"
        trial = 0
        for csvFilename in csvFilenameList:
            # creates csvFilename.hex from file in hdfs dir 
            csvPathname = importFolderPath + "/" + csvFilename

            timeoutSecs = 1000
            # do an import first, because we want to get the size of the file
            (importResult, importPattern) = h2i.import_only(path=csvPathname, schema="maprfs", timeoutSecs=timeoutSecs)
            print "importResult:", h2o.dump_json(importResult)
            succeeded = importResult['files']
            fails = importResult['fails']

            if len(succeeded) < 1:
                raise Exception("Should have imported at least 1 key for %s" % csvPathname)

            # just do a search
            foundIt = None
            for f in succeeded:
                if csvPathname in f:
                    foundIt = f
                    break

            if not foundIt:
                raise Exception("Should have found %s in the imported keys for %s" % (importPattern, csvPathname))

            totalBytes = 0

            print "Loading", csvFilename, 'from maprfs'
            start = time.time()
            parseResult = h2i.import_parse(path=csvPathname, schema="maprfs", timeoutSecs=timeoutSecs, pollTimeoutSecs=360,
                doSummary=False, benchmarkLogging=benchmarkLogging)
            print "parse result:", parseResult['destination_key']

            elapsed = time.time() - start
            fileMBS = (totalBytes/1e6)/elapsed
            l = '{!s} jvms, {!s}GB heap, {:s} {:s} for {:.2f} secs'.format(
                len(h2o.nodes), h2o.nodes[0].java_heap_GB, 'Parse', csvPathname, elapsed)
            print "\n"+l
            h2o.cloudPerfH2O.message(l)

            if DO_RF:
                print "\n" + csvFilename
                start = time.time()
                kwargs = {
                    'ntrees': 1
                    }
                paramsString = json.dumps(kwargs)
                RFview = h2o_cmd.runRF(parseResult=parseResult, timeoutSecs=2000,
                    benchmarkLogging=benchmarkLogging, **kwargs)
                elapsed = time.time() - start
                print "rf end on ", csvPathname, 'took', elapsed, 'seconds.', "%d pct. of timeout" % ((elapsed/timeoutSecs) * 100)

                l = '{!s} jvms, {!s}GB heap, {:s} {:s} {:s} for {:.2f} secs {:s}' .format(
                    len(h2o.nodes), h2o.nodes[0].java_heap_GB, "RF", "trial "+str(trial), csvFilename, elapsed, paramsString)
                print l
                h2o.cloudPerfH2O.message(l)

            print "Deleting all keys, to make sure our parse times don't include spills"
            h2i.delete_keys_at_all_nodes()
            trial += 1

if __name__ == '__main__':
    h2o.unit_main()
