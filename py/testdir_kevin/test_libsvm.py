import unittest
import random, sys, time, os
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i, h2o_kmeans

DO_KMEANS = False
# PARSER_TYPE = None
PARSER_TYPE = 'SVMLight'

print "This parses as enum if parser_type=SVMLight isn't specified"
print "Gets error if parser_type=SVMLight is specified"

def write_syn_dataset(csvPathname, trial):
    dsf = open(csvPathname, "w+")

    # test data from customer with multiple spaces removed
    rowData1 = """
1.0 0:1.0 1:1.0 2:1.0 3:75.0 4:512.0 5:1.0 6:0.0 7:0.0 8:0.0 9:0.0 10:0.0 11:0.0 12:9.0
0.0 0:1.0 1:1.0 2:1.0 3:0.0 4:0.0 5:0.0 6:0.0 7:0.0 8:1.0 9:0.0 10:0.0 11:0.0 12:3.0
0.0 0:1.0 1:1.0 2:1.0 3:0.0 4:30.0 5:0.0 6:0.0 7:1.0 8:0.0 9:0.0 10:0.0 11:0.0 12:32.0
0.0 0:1.0 1:1.0 2:1.0 3:0.0 4:0.0 5:0.0 6:0.0 7:0.0 8:0.0 9:0.0 10:0.0 11:0.0 12:1.0
0.0 1:1.0 2:1.0 3:0.0 4:25.0 5:0.0 6:0.0 7:1.0 8:0.0 9:0.0 10:0.0 11:0.0 12:27.0 13:1.0
"""
    # test data from customer
    rowData2 = """
1.0     0:1.0   1:1.0   2:1.0   3:75.0  4:512.0 5:1.0   6:0.0   7:0.0   8:0.0   9:0.0   10:0.0  11:0.0  12:9.0
0.0     0:1.0   1:1.0   2:1.0   3:0.0   4:0.0   5:0.0   6:0.0   7:0.0   8:1.0   9:0.0   10:0.0  11:0.0  12:3.0
0.0     0:1.0   1:1.0   2:1.0   3:0.0   4:30.0  5:0.0   6:0.0   7:1.0   8:0.0   9:0.0   10:0.0  11:0.0  12:32.0
0.0     0:1.0   1:1.0   2:1.0   3:0.0   4:0.0   5:0.0   6:0.0   7:0.0   8:0.0   9:0.0   10:0.0  11:0.0  12:1.0
0.0     1:1.0   2:1.0   3:0.0   4:25.0  5:0.0   6:0.0   7:1.0   8:0.0   9:0.0   10:0.0  11:0.0  12:27.0 13:1.0
"""

    # alternate between the two cases
    if trial%2 == 0:
        rowData = rowData1
    else:
        rowData = rowData2

    dsf.write(rowData)
    dsf.close()

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = h2o.setup_random_seed()
        h2o.init(java_heap_GB=1)

    @classmethod
    def tearDownClass(cls):
        # wait while I inspect things
        # time.sleep(1500)
        h2o.tear_down_cloud()

    def test_libsvm(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()

        for trial in range(2):
            csvFilename = "syn_ints.csv"
            hex_key = "1.hex"
            csvPathname = SYNDATASETS_DIR + '/' + csvFilename
            write_syn_dataset(csvPathname, trial)
            timeoutSecs = 10
        
            # have to import each time, because h2o deletes source after parse

            # PARSE******************************************
            # creates csvFilename.hex from file in importFolder dir 
            # parseResult = h2i.import_parse(path=csvPathname, parser_type='SVMLight', hex_key=hex_key, timeoutSecs=2000)
            parseResult = h2i.import_parse(parser_type=PARSER_TYPE, path=csvPathname, hex_key=hex_key, timeoutSecs=2000)

            # INSPECT******************************************
            start = time.time()
            inspect = h2o_cmd.runInspect(key=hex_key, timeoutSecs=360)
            print "Inspect:", hex_key, "took", time.time() - start, "seconds"
            h2o_cmd.infoFromInspect(inspect, csvFilename)
            numRows = inspect['numRows']
            numCols = inspect['numCols']
            summaryResult = h2o_cmd.runSummary(key=hex_key)
            h2o_cmd.infoFromSummary(summaryResult)

            if DO_KMEANS:
                # KMEANS******************************************
                kwargs = {
                    'k': 3, 
                    'initialization': 'Furthest',
                    'ignored_cols': None, #range(11, numCols), # THIS BREAKS THE REST API
                    'max_iter': 10,
                    # 'normalize': 0,
                    # reuse the same seed, to get deterministic results (otherwise sometimes fails
                    'seed': 265211114317615310,
                }

                # fails if I put this in kwargs..i.e. source = dest
                # 'destination_key': parseResult['destination_key'],

                timeoutSecs = 600
                start = time.time()
                kmeans = h2o_cmd.runKMeans(parseResult=parseResult, timeoutSecs=timeoutSecs, **kwargs)
                elapsed = time.time() - start
                print "kmeans end on ", csvPathname, 'took', elapsed, 'seconds.', \
                    "%d pct. of timeout" % ((elapsed/timeoutSecs) * 100)
                # this does an inspect of the model and prints the clusters
                h2o_kmeans.simpleCheckKMeans(self, kmeans, **kwargs)

                (centers, tupleResultList) = h2o_kmeans.bigCheckResults(self, kmeans, csvPathname, parseResult, 'd', **kwargs)


if __name__ == '__main__':
    h2o.unit_main()
