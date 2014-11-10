import unittest, time, sys, random
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        # assume we're at 0xdata with it's hdfs namenode
        h2o.init(1, use_hdfs=True, hdfs_version='cdh4', hdfs_name_node='mr-0x6')

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_hdfs_cdh4_fvec(self):
        print "\nLoad a list of files from HDFS, parse and do 1 RF tree"
        print "\nYou can try running as hduser/hduser if fail"
        # larger set in my local dir
        # fails because classes aren't integers
        #    "allstate_claim_prediction_train_set.zip",
        csvFilenameAll = [
            "TEST-poker1000.csv",
            "leads.csv",
            "and-testing.data",
            "arcene2_train.both",
            "arcene_train.both",
            # these can't RF ..output classes not integer?
            # "bestbuy_test.csv",
            # "bestbuy_train.csv",
            "covtype.data",
            "covtype.4x.shuffle.data",
            "covtype4x.shuffle.data",
            "covtype.13x.data",
            "covtype.13x.shuffle.data",
            # "covtype.169x.data",
            # "prostate_2g.csv",
            # "prostate_long.csv.gz",
            "prostate_long_1G.csv",
            "hhp.unbalanced.012.1x11.data.gz",
            "hhp.unbalanced.012.data.gz",
            "hhp.unbalanced.data.gz",
            "hhp2.os.noisy.0_1.data",
            "hhp2.os.noisy.9_4.data",
            "hhp_9_14_12.data",
            # "poker_c1s1_testing_refresh.csv",
            # "3G_poker_shuffle",
            # "billion_rows.csv.gz",
            # "poker-hand.1244M.shuffled311M.full.txt",
        ]

        # pick 8 randomly!
        if (1==0):
            csvFilenameList = random.sample(csvFilenameAll,8)
        # Alternatively: do the list in order! Note the order is easy to hard
        else:
            csvFilenameList = csvFilenameAll

        # pop open a browser on the cloud
        # h2b.browseTheCloud()

        for csvFilename in csvFilenameList:
            # creates csvFilename.hex from file in hdfs dir 
            print "Loading", csvFilename, 'from HDFS'
            csvPathname = "datasets/" + csvFilename
            parseResult = h2i.import_parse(path=csvPathname, schema='hdfs', header=0, timeoutSecs=1000)
            print "parse result:", parseResult['destination_key']

if __name__ == '__main__':
    h2o.unit_main()
