import unittest, time, sys, time, random
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_hosts, h2o_browse as h2b, h2o_import as h2i, h2o_common

print "Assumes you ran ../build_for_clone.py in this directory"
print "Using h2o-nodes.json. Also the sandbox dir"
class releaseTest(h2o_common.ReleaseCommon, unittest.TestCase):

    def test_c6_maprfs(self):
        print "\nLoad a list of files from HDFS, parse and do 1 RF tree"
        print "\nYou can try running as hduser/hduser if fail"
        # larger set in my local dir
        # fails because classes aren't integers
        #    "allstate_claim_prediction_train_set.zip",
        csvFilenameAll = [
            "allyears2k.csv",
            "billion_rows.csv.gz",
            "covtype.data",
            "covtype.shuffled.data",
            "covtype200x.data",
            "covtype20x.data",
            "kddcup_1999.data.gz",
            "rand_logreg_100000000x70.csv.gz",
        ]

        # pick 8 randomly!
        if (1==0):
            csvFilenameList = random.sample(csvFilenameAll,8)
        # Alternatively: do the list in order! Note the order is easy to hard
        else:
            csvFilenameList = csvFilenameAll

        # pop open a browser on the cloud
        # h2b.browseTheCloud()

        # save the first, for all comparisions, to avoid slow drift with each iteration
        for csvFilename in csvFilenameList:
            # creates csvFilename.hex from file in hdfs dir 
            print "Loading", csvFilename, 'from HDFS'
            parseResult = h2i.import_parse(path="datasets/standard/" + csvFilename, schema="maprfs", timeoutSecs=1000)
            print csvFilename, 'parse time:', parseResult['response']['time']
            print "parse result:", parseResult['destination_key']

            print "\n" + csvFilename
            start = time.time()
            RFview = h2o_cmd.runRF(trees=1,parseResult=parseResult,timeoutSecs=2000)
            # h2b.browseJsonHistoryAsUrlLastMatch("RFView")

if __name__ == '__main__':
    h2o.unit_main()
