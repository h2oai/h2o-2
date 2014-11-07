import unittest, random, sys, time, os
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_glm, h2o_import as h2i, h2o_exec as h2e

# FIX!. enums may only work if 0 based
# try -5,5 etc
# maybe call GLM on it after doing factor (check # of coefficients)

DO_CASE = 1
REBALANCE_CHUNKS = 100

def write_syn_dataset(csvPathname, rowCount, colCount, SEED):
    r1 = random.Random(SEED)
    dsf = open(csvPathname, "w+")

    for i in range(rowCount):
        rowData = []
        for j in range(colCount):
            ri1 = int(r1.triangular(0,4,2.5))
            rowData.append(ri1)

        rowTotal = sum(rowData)

        ### print colCount, rowTotal, result
        rowDataStr = map(str,rowData)
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

    def test_rebalance_int2enum(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        tryList = [
            (100000,  30, 'cC', 100),
            ]

        for (rowCount, colCount, hex_key, timeoutSecs) in tryList:
            SEEDPERFILE = random.randint(0, sys.maxint)
            csvFilename = 'syn_' + str(SEEDPERFILE) + "_" + str(rowCount) + 'x' + str(colCount) + '.csv'
            csvPathname = SYNDATASETS_DIR + '/' + csvFilename

            print "\nCreating random", csvPathname
            write_syn_dataset(csvPathname, rowCount, colCount, SEEDPERFILE)
            parseResult = h2i.import_parse(path=csvPathname, schema='put', hex_key=hex_key, timeoutSecs=20)
            hex_key=parseResult['destination_key']
            inspect = h2o_cmd.runInspect(key=hex_key)
            print "\n" + csvFilename


            print "Rebalancing it to create an artificially large # of chunks"
            rb_key = "rb_%s" % (hex_key)
            start = time.time()
            print "Rebalancing %s to %s with %s chunks" % (hex_key, rb_key, REBALANCE_CHUNKS)
            rebalanceResult = h2o.nodes[0].rebalance(source=hex_key, after=rb_key, chunks=REBALANCE_CHUNKS)
            elapsed = time.time() - start
            print "rebalance end on ", csvFilename, 'took', elapsed, 'seconds',\

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
                    raise Exception("column %s, which has name %s, didn't convert to Enum, is %s %s" (column_index, colname, stattype, coltype))
                if nacnt!=0:
                    raise Exception("column %s, which has name %s, somehow got NAs after convert to Enum  %s" (column_index, colname, nacnt))
                if cardinality!=4:
                    raise Exception("column %s, which has name %s,  should have cardinality 4, got: %s" (column_index, colname, cardinality))
                h2o_cmd.infoFromSummary(summaryResult)


if __name__ == '__main__':
    h2o.unit_main()
