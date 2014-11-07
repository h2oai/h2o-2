import unittest, random, sys, time
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i, h2o_exec as h2e


print "Slice many rows"
def write_syn_dataset(csvPathname, rowCount, colCount, SEED):
    # 8 random generatators, 1 per column
    r1 = random.Random(SEED)
    dsf = open(csvPathname, "w+")

    for i in range(rowCount):
        rowData = []
        for j in range(colCount):
            r = r1.randint(0,1)
            rowData.append(r)

        rowDataCsv = ",".join(map(str,rowData))
        dsf.write(rowDataCsv + "\n")

    dsf.close()

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = h2o.setup_random_seed()
        h2o.init(1,java_heap_GB=14)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_exec2_row_range(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        tryList = [
            (1000000, 5, 'cA', 200),
            ]

        # h2b.browseTheCloud()
        for (rowCount, colCount, hex_key, timeoutSecs) in tryList:
            SEEDPERFILE = random.randint(0, sys.maxint)

            csvFilename = 'syn_' + str(SEEDPERFILE) + "_" + str(rowCount) + 'x' + str(colCount) + '.csv'
            csvPathname = SYNDATASETS_DIR + '/' + csvFilename

            print "\nCreating random", csvPathname
            write_syn_dataset(csvPathname, rowCount, colCount, SEEDPERFILE)

            start = time.time()
            parseResult = h2i.import_parse(path=csvPathname, schema='put', hex_key=hex_key, 
                timeoutSecs=timeoutSecs, doSummary=False)
            print "Parse:", parseResult['destination_key'], "took", time.time() - start, "seconds"

            inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
            h2o_cmd.infoFromInspect(inspect, csvPathname)
            print "\n" + csvPathname, \
                "    numRows:", "{:,}".format(inspect['numRows']), \
                "    numCols:", "{:,}".format(inspect['numCols'])

            # should match # of cols in header or ??
            self.assertEqual(inspect['numCols'], colCount,
                "parse created result with the wrong number of cols %s %s" % (inspect['numCols'], colCount))
            self.assertEqual(inspect['numRows'], rowCount,
                "parse created result with the wrong number of rows (header shouldn't count) %s %s" % \
                (inspect['numRows'], rowCount))

            REPEAT = 1
            for i in range(REPEAT):
                hex_key_i = hex_key + "_" + str(i)
                execExpr = "%s=%s[1,]" % (hex_key_i, hex_key)
                resultExec, result = h2e.exec_expr(execExpr=execExpr, timeoutSecs=30)

                execExpr = "%s=%s[1:%s,]" % (hex_key_i, hex_key, 100)
                resultExec, result = h2e.exec_expr(execExpr=execExpr, timeoutSecs=30)

                execExpr = "%s=%s[1:%s,]" % (hex_key_i, hex_key, rowCount-10)
                resultExec, result = h2e.exec_expr(execExpr=execExpr, timeoutSecs=30)
                inspect = h2o_cmd.runInspect(None, hex_key_i, timeoutSecs=timeoutSecs)
                h2o_cmd.infoFromInspect(inspect, hex_key_i)
                print "\n" + hex_key_i, \
                    "    numRows:", "{:,}".format(inspect['numRows']), \
                    "    numCols:", "{:,}".format(inspect['numCols'])

if __name__ == '__main__':
    h2o.unit_main()
