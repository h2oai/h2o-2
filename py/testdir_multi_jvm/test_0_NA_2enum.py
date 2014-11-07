import unittest, random, sys, time, os
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_glm, h2o_import as h2i, h2o_exec as h2e

# all zeroes, the summary looks like
# column: {
#   "colname": "C1", 
#   "hbrk": null, 
#   "hcnt": [
#     100
#   ], 
#   "hstart": 0.0, 
#   "hstep": 1.0, 
#   "nacnt": 0, 
#   "stats": {
#     "cardinality": 1, 
#     "type": "Enum"
#   }, 
#   "type": null
# }

# FIX!. enums may only work if 0 based
# try -5,5 etc
# maybe call GLM on it after doing factor (check # of coefficients)

print "This only tests mixed 0 and NA. All NA or All 0 might be different"
DO_REBALANCE = False
REBALANCE_CHUNKS = 100

# zero can be 0 (int) or 0.0000? (numeric?) 
def write_syn_dataset(csvPathname, rowCount, colCount, zero, SEED):
    r1 = random.Random(SEED)
    dsf = open(csvPathname, "w+")

    for i in range(rowCount):
        rowDataStr = []
        for j in range(colCount):
            ri1 = int(r1.triangular(0,2,0.75))
            if ri1==1:
                ri1 = 'NA'
            else:
                ri1 = zero
            
            rowDataStr.append(ri1)
        rowDataCsv = ",".join(rowDataStr)
        dsf.write(rowDataCsv + "\n")

    dsf.close()


class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = h2o.setup_random_seed()
        h2o.init(3)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_0_NA_2enum(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        tryList = [
            (100,  30, '0', 'cC', 100),
            (100,  30, '0.0', 'cC', 100),
            (100,  30, '0.0000000', 'cC', 100),
            ]

        for (rowCount, colCount, zero, hex_key, timeoutSecs) in tryList:
            SEEDPERFILE = random.randint(0, sys.maxint)
            csvFilename = 'syn_' + str(SEEDPERFILE) + "_" + str(rowCount) + 'x' + str(colCount) + '.csv'
            csvPathname = SYNDATASETS_DIR + '/' + csvFilename

            print "\nCreating random", csvPathname
            write_syn_dataset(csvPathname, rowCount, colCount, zero, SEEDPERFILE)
            parseResult = h2i.import_parse(path=csvPathname, schema='put', hex_key=hex_key, timeoutSecs=10)
            print "Parse result['destination_key']:", parseResult['destination_key']
            inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
            print "\n" + csvFilename


            if DO_REBALANCE:
                print "Rebalancing it to create an artificially large # of chunks"
                rb_key = "rb_%s" % hex_key
                start = time.time()
                print "Rebalancing %s to %s with %s chunks" % (hex_key, rb_key, REBALANCE_CHUNKS)
                rebalanceResult = h2o.nodes[0].rebalance(source=hex_key, after=rb_key, chunks=REBALANCE_CHUNKS)
                elapsed = time.time() - start
                print "rebalance end on ", csvFilename, 'took', elapsed, 'seconds'
            else:
                rb_key = hex_key

            print "Now doing to_enum across all columns of %s" % hex_key
            for column_index in range(colCount):
                # is the column index 1-base in to_enum
                result = h2o.nodes[0].to_enum(None, src_key=hex_key, column_index=column_index+1)
                # print "\nto_enum result:", h2o.dump_json(result)
                summaryResult = h2o_cmd.runSummary(key=hex_key)
                # check that it at least is an enum column now, with no na's
                # just look at the column we touched
                column = summaryResult['summaries'][column_index]
                colname = column['colname']
                coltype = column['type']
                nacnt = column['nacnt']
                stats = column['stats']
                stattype = stats['type']
                cardinality = stats['cardinality']
                if stattype != 'Enum':
                    raise Exception("column %s, which has name %s, didn't convert to Enum, is %s %s" % (column_index, colname, stattype, coltype))
                # I'm generating NA's ..so it should be > 0. .but it could be zero . I guess i have enough rows to get at least 1
                if nacnt<=0 or nacnt>rowCount:
                    raise Exception("column %s, which has name %s, somehow got NA cnt wrong after convert to Enum  %s %s" % 
                        (column_index, colname, nacnt, rowCount))
                if cardinality!=1: # NAs don't count?
                    # print "stats:", h2o.dump_json(stats)
                    print "column:", h2o.dump_json(column)
                    raise Exception("column %s, which has name %s, should have cardinality 1, got: %s" % (column_index, colname, cardinality))
                h2o_cmd.infoFromSummary(summaryResult)



if __name__ == '__main__':
    h2o.unit_main()
