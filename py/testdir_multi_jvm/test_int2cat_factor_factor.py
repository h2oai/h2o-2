import unittest
import random, sys, time, os
sys.path.extend(['.','..','py'])

# FIX! add cases with shuffled data!
import h2o, h2o_cmd, h2o_hosts, h2o_glm
import h2o_browse as h2b, h2o_import as h2i, h2o_exec as h2e

def write_syn_dataset(csvPathname, rowCount, colCount, SEED):
    r1 = random.Random(SEED)
    r2 = random.Random(SEED)
    dsf = open(csvPathname, "w+")

    for i in range(rowCount):
        rowData = []
        for j in range(colCount):
            ri1 = int(r1.triangular(0,4,2.5))
            rowData.append(ri1)

        rowTotal = sum(rowData)
        result = r2.randint(0,1)

        ### print colCount, rowTotal, result
        rowDataStr = map(str,rowData)
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
        SEED = random.randint(0, sys.maxint)
        # SEED = 
        random.seed(SEED)
        print "\nUsing random seed:", SEED
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(3)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_int2cat_factor_factor(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        tryList = [
            (10000,  10, 'cA.hex', 100),
            (10000,  20, 'cB.hex', 200),
            (10000,  30, 'cC.hex', 300),
            (10000,  40, 'cD.hex', 400),
            (10000,  50, 'cE.hex', 500),
            ]

        ### h2b.browseTheCloud()

        # we're going to do a special exec across all the columns to turn them into enums
        # including the duplicate of the output!
        exprList = [
                '<keyX>= colSwap(<keyX>,<col1>,factor(<keyX>[0]))', 
                ### '<keyX>= colSwap(<keyX>,<col1>,<keyX>[0])',
                ### '<keyX>= colSwap(<keyX>,<col1>,factor(<keyX>[<col1>]))', 
                ### '<keyX>= colSwap(<keyX>,<col1>,<keyX>[<col1>])',
            ]

        for (rowCount, colCount, key2, timeoutSecs) in tryList:
            SEEDPERFILE = random.randint(0, sys.maxint)
            csvFilename = 'syn_' + str(SEEDPERFILE) + "_" + str(rowCount) + 'x' + str(colCount) + '.csv'
            csvPathname = SYNDATASETS_DIR + '/' + csvFilename

            print "\nCreating random", csvPathname
            write_syn_dataset(csvPathname, rowCount, colCount, SEEDPERFILE)
            parseKey = h2o_cmd.parseFile(None, csvPathname, key2=key2, timeoutSecs=90)
            print csvFilename, 'parse time:', parseKey['response']['time']
            print "Parse result['destination_key']:", parseKey['destination_key']

            inspect = h2o_cmd.runInspect(None, parseKey['destination_key'])
            print "\n" + csvFilename

            print "\nNow running the int 2 enum exec command across all input cols"
            colResultList = h2e.exec_expr_list_across_cols(None, exprList, key2, maxCol=colCount, 
                timeoutSecs=90, incrementingResult=False)
            print "\nexec colResultList", colResultList

            if not h2o.browse_disable:
                h2b.browseJsonHistoryAsUrlLastMatch("Inspect")
                time.sleep(3)

if __name__ == '__main__':
    h2o.unit_main()
