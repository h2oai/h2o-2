import unittest, random, sys, time
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i, h2o_exec as h2e

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

    def test_parse_65k_cols_01_fvec(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        tryList = [
            (10, 63000, 'cH', 100),
            (10, 65000, 'cH', 100),
            ]

        # h2b.browseTheCloud()
        for (rowCount, colCount, hex_key, timeoutSecs) in tryList:
            SEEDPERFILE = random.randint(0, sys.maxint)

            csvFilename = 'syn_' + str(SEEDPERFILE) + "_" + str(rowCount) + 'x' + str(colCount) + '.csv'
            csvPathname = SYNDATASETS_DIR + '/' + csvFilename

            print "Creating random", csvPathname
            write_syn_dataset(csvPathname, rowCount, colCount, SEEDPERFILE)

            start = time.time()
            parseResult = h2i.import_parse(path=csvPathname, schema='put', hex_key=hex_key, 
                timeoutSecs=timeoutSecs, doSummary=False)
            print "Parse", parseResult['destination_key'], "took", time.time() - start, "seconds"
            print "Summary should work with 65k"
            start = time.time()
            h2o_cmd.runSummary(key=parseResult['destination_key'], timeoutSecs=300)
            print "Summary", parseResult['destination_key'], "took", time.time() - start, "seconds"

            # We should be able to see the parse result?
            start = time.time()
            inspect = h2o_cmd.runInspect(None, parseResult['destination_key'], timeoutSecs=timeoutSecs)
            print "Inspect:", parseResult['destination_key'], "took", time.time() - start, "seconds"
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

            

            # we should obey max_column_display
            # column_limits = [25, 25000]
            # for column_limit in column_limits:
            #     inspect = h2o_cmd.runInspect(None, parseResult['destination_key'], max_column_display=column_limit, timeoutSecs=timeoutSecs)
            #     self.assertEqual(len( inspect['cols'] ) , column_limit, "inspect doesn't obey max_column_display = %s" % column_limit)
            #     for r in range(0, len( inspect[ 'rows' ] )):
            #         # NB: +1 below because each row includes a row header row: #{row}
            #         self.assertEqual(len( inspect['rows'][r] ) , column_limit + 1, "inspect data rows doesn't obey max_column_display = " + str(column_limit))


             # if not h2o.browse_disable:
             #    h2b.browseJsonHistoryAsUrlLastMatch("Inspect")
             #    time.sleep(5)

if __name__ == '__main__':
    h2o.unit_main()
