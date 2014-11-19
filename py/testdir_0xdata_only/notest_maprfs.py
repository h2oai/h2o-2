import unittest, time, sys, time, random, json
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i

DO_RF = False
class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        # assume we're at 0xdata with it's hdfs namenode
        # hdfs_config='/opt/mapr/conf/mapr-clusters.conf', 
        #        # hdfs_name_node='mr-0x1.0xdata.loc:7222')
        #        hdfs_version='mapr2.1.3',
        print "This doesn't work. we don't package mapr files with h2o"
        h2o.init(1, 
            java_heap_GB=15,
            enable_benchmark_log=True,
            use_maprfs=True, 
            hdfs_version='mapr3.1.1',
            hdfs_name_node='mr-0x2:7222')

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_B_hdfs_files(self):
        print "\nLoad a list of files from HDFS, parse and do 1 RF tree"
        print "\nYou can try running as hduser/hduser if fail"
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
            "hhp2.os.noisy.0_1.data",
            "hhp2.os.noisy.9_4.data",
            "hhp_9_14_12.data",
            "leads.csv",
            "prostate_long_1G.csv",
        ]

        # pick 8 randomly!
        if (1==0):
            csvFilenameList = random.sample(csvFilenameAll,8)
        # Alternatively: do the list in order! Note the order is easy to hard
        else:
            csvFilenameList = csvFilenameAll

        # pop open a browser on the cloud
        # h2b.browseTheCloud()

        benchmarkLogging = ['cpu','disk', 'network', 'iostats', 'jstack']
        benchmarkLogging = ['cpu','disk', 'network', 'iostats']
        # IOStatus can hang?
        benchmarkLogging = ['cpu', 'disk', 'network']
        benchmarkLogging = []

        # save the first, for all comparisions, to avoid slow drift with each iteration
        importFolderPath = "datasets"
        trial = 0
        for csvFilename in csvFilenameList:
            # creates csvFilename.hex from file in hdfs dir 
            csvPathname = importFolderPath + "/" + csvFilename

            timeoutSecs = 1000

            # do an import first, because we want to get the size of the file
            (importResult, importPattern) = h2i.import_only(path=csvPathname, schema="maprfs", timeoutSecs=timeoutSecs)
            succeeded = importResult['succeeded']
            if len(succeeded) < 1:
                raise Exception("Should have imported at least 1 key for" % csvPathname)

            # just do a search
            foundIt = None
            for f in succeeded:
                if csvPathname in f['key']:
                    foundIt = f
                    break

            if foundIt:
                value_size_bytes = f['value_size_bytes']
            else:
                raise Exception("Should have found %s in the imported keys for %s" % (importPattern, csvPathname))

            # no pattern matching, so no multiple files to add up
            totalBytes = value_size_bytes

            #  "succeeded": [
            #    {
            #      "file": "maprfs://172.16.2.171:7222/datasets/prostate_long_1G.csv", 
            #      "key": "maprfs://172.16.2.171:7222/datasets/prostate_long_1G.csv", 
            #      "value_size_bytes": 1115287100
            #    },

            print "Loading", csvFilename, 'from maprfs'
            start = time.time()
            parseResult = h2i.import_parse(path=csvPathname, schema="maprfs", timeoutSecs=timeoutSecs, 
                doSummary=False, benchmarkLogging=benchmarkLogging)
            print "parse result:", parseResult['destination_key']

            elapsed = time.time() - start
            fileMBS = (totalBytes/1e6)/elapsed
            l = '{!s} jvms, {!s}GB heap, {:s} {:s} {:6.2f}MB {:6.2f} MB/sec for {:.2f} secs'.format(
                len(h2o.nodes), h2o.nodes[0].java_heap_GB, 'Parse', csvPathname, (totalBytes+0.0)/1e6, fileMBS, elapsed)
            print "\n"+l
            h2o.cloudPerfH2O.message(l)

            if DO_RF:
                print "\n" + csvFilename
                start = time.time()
                kwargs = {
                    'ntree': 1
                    }
                paramsString = json.dumps(kwargs)
                RFview = h2o_cmd.runRF(parseResult=parseResult, timeoutSecs=2000, 
                    benchmarkLogging=benchmarkLogging, **kwargs)
                elapsed = time.time() - start
                print "rf end on ", csvPathname, 'took', elapsed, 'seconds.', "%d pct. of timeout" % ((elapsed/timeoutSecs) * 100)

                l = '{!s} jvms, {!s}GB heap, {:s} {:s} {:s} for {:.2f} secs {:s}' .format(
                    len(h2o.nodes), h2o.nodes[0].java_heap_GB, "KMeans", "trial "+str(trial), csvFilename, elapsed, paramsString)
                print l
                h2o.cloudPerfH2O.message(l)

            print "Deleting all keys, to make sure our parse times don't include spills"
            h2i.delete_keys_at_all_nodes()

            trial += 1

if __name__ == '__main__':
    h2o.unit_main()
