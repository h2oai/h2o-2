import unittest, random, sys, time
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_glm, h2o_browse as h2b, h2o_import as h2i, h2o_exec as h2e

def write_syn_dataset(csvPathname, rowCount, colCount, SEED):
    r1 = random.Random(SEED)
    dsf = open(csvPathname, "w+")

    for i in range(rowCount):
        rowData = []
        for j in range(colCount):
            ri1 = int(r1.triangular(0,4,2.5))
            rowData.append(ri1)

        rowTotal = sum(rowData)

        if (rowTotal > (1.6 * colCount)): 
            result = 1
        else:
            result = 0

        ### print colCount, rowTotal, result
        rowDataStr = map(str,rowData)
        rowDataStr.append(str(result))
        # add the output twice, to try to match to it?
        rowDataStr.append(str(result))

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

    def test_exec2_int2cat(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        tryList = [
            (100000,  10, 'cA', 100),
            (100000,  20, 'cB', 100),
            (100000,  30, 'cC', 100),
            (100000,  40, 'cD', 100),
            (100000,  10, 'cE', 100),
            (100000,  20, 'cF', 100),
            (100000,  30, 'cG', 100),
            (100000,  40, 'cH', 100),
            ]

        ### h2b.browseTheCloud()
        lenNodes = len(h2o.nodes)

        # we're going to do a special exec across all the columns to turn them into enums
        # including the duplicate of the output!
        exprList = [
                '<keyX>[,<col1>] = factor(<keyX>[,<col1>])',
            ]

        for (rowCount, colCount, hex_key, timeoutSecs) in tryList:
            SEEDPERFILE = random.randint(0, sys.maxint)
            csvFilename = 'syn_' + str(SEEDPERFILE) + "_" + str(rowCount) + 'x' + str(colCount) + '.csv'
            csvPathname = SYNDATASETS_DIR + '/' + csvFilename

            print "\nCreating random", csvPathname
            write_syn_dataset(csvPathname, rowCount, colCount, SEEDPERFILE)
            parseResult = h2i.import_parse(path=csvPathname, schema='put', hex_key=hex_key, timeoutSecs=10)
            print "Parse result['destination_key']:", parseResult['destination_key']

            inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
            print "\n" + csvFilename

            print "\nNow running the int 2 enum exec command across all input cols"
            colResultList = h2e.exec_expr_list_across_cols(None, exprList, hex_key, maxCol=colCount, 
                timeoutSecs=4, incrementingResult=False)
            print "\nexec colResultList", colResultList

            if not h2o.browse_disable:
                h2b.browseJsonHistoryAsUrlLastMatch("Inspect")
                time.sleep(3)

if __name__ == '__main__':
    h2o.unit_main()
