import unittest, time, sys, random
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.init(3, use_hdfs=True, hdfs_version='cdh4', hdfs_name_node='mr-0x6')

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_B_hdfs_files(self):
        # larger set in my local dir
        # fails because classes aren't integers
        #    "allstate_claim_prediction_train_set.zip",
        csvFilenameAll = [
            "3G_poker_shuffle",
            "TEST-poker1000.csv",
            # corrupt zip file?
            # "allstate_claim_prediction_train_set.zip",
            "and-testing.data",
            "arcene2_train.both",
            "arcene_train.both",
            "bestbuy_test.csv",
            "bestbuy_train.csv",
            "billion_rows.csv.gz",
            "covtype.13x.data",
            "covtype.13x.shuffle.data",
            "covtype.169x.data",
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
        h2b.browseTheCloud()

        timeoutSecs = 1000
        # save the first, for all comparisions, to avoid slow drift with each iteration
        firstglm = {}

        for csvFilename in csvFilenameList:
            # creates csvFilename.hex from file in hdfs dir 
            start = time.time()
            print 'Parsing', csvFilename
            csvPathname = "datasets/" + csvFilename
            parseResult = h2i.import_parse(path=csvPathname, schema='hdfs', header=0,
                timeoutSecs=timeoutSecs, retryDelaySecs=1.0)
            print csvFilename, '\nparse time (python)', time.time() - start, 'seconds'
            ### print h2o.dump_json(parseResult['response'])

            print "parse result:", parseResult['destination_key']
            # I use this if i want the larger set in my localdir
            inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
            h2o_cmd.infoFromInspect(inspect, csvPathname)

            # h2b.browseJsonHistoryAsUrlLastMatch("Inspect")
            print "\n" + csvFilename

if __name__ == '__main__':
    h2o.unit_main()
