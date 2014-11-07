import unittest, random, sys, time
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i, h2o_exec as h2e, h2o_util
import h2o_browse as h2b

print "Create csv with lots of same data (98% 0?), so gz will have high compression ratio"

ONE_RATE = 0.00001
DOSUMMARY = False
# ONE_RATE = 0.1
def write_syn_dataset(csvPathname, rowCount, colCount, SEED):
    # 8 random generatators, 1 per column
    r1 = random.Random(SEED)
    dsf = open(csvPathname, "w+")

    for i in range(rowCount):
        rowData = []
        for j in range(colCount):
            # r = h2o_util.choice_with_probability([(1.1, .02), (0.1, .98)])
            r = h2o_util.choice_with_probability([(1, ONE_RATE), (0, 1 - ONE_RATE)])
            # make r a many-digit real, so gzip compresses even more better!
            # rowData.append('%#034.32e' % r)
            rowData.append('%.1f' % r)

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
        h2o.init(1,java_heap_GB=4)
        # h2b.browseTheCloud()

    @classmethod
    def tearDownClass(cls):
        # time.sleep(3600)
        h2o.tear_down_cloud()

    def test_parse_syn_gz_cat(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        tryList = [
            # summary fails with 100000 cols
            # overwrite the key each time to save space?
            (100, 100, 'cF', 600),
            (100, 5000, 'cF', 600),
            (100, 10000, 'cF', 600),
            # (100, 12000, 'cF', 600),
            # (100, 15000, 'cF', 600),
            # (100, 17000, 'cF', 600),
            (100, 20000, 'cF', 600),
            (100, 40000, 'cF', 600),
            ]

        # h2b.browseTheCloud()
        for (rowCount, colCount, hex_key, timeoutSecs) in tryList:
            SEEDPERFILE = random.randint(0, sys.maxint)

            csvFilename = 'syn_' + str(SEEDPERFILE) + "_" + str(rowCount) + 'x' + str(colCount) + '.csv'
            csvPathname = SYNDATASETS_DIR + '/' + csvFilename

            print "Creating random", csvPathname
            write_syn_dataset(csvPathname, rowCount, colCount, SEEDPERFILE)

            csvFilenamegz = csvFilename + ".gz"
            csvPathnamegz = SYNDATASETS_DIR + '/' + csvFilenamegz
            h2o_util.file_gzip(csvPathname, csvPathnamegz)

            start = time.time()
            print "Parse start:", csvPathnamegz
            parseResult = h2i.import_parse(path=csvPathnamegz, schema='put', hex_key=hex_key, 
                timeoutSecs=timeoutSecs, doSummary=DOSUMMARY)
            if DOSUMMARY:
                algo = "Parse and Summary:"
            else:
                algo = "Parse:"
            print algo , parseResult['destination_key'], "took", time.time() - start, "seconds"

            print "Inspecting.."
            start = time.time()
            inspect = h2o_cmd.runInspect(key=parseResult['destination_key'], timeoutSecs=timeoutSecs)
            print "Inspect:", parseResult['destination_key'], "took", time.time() - start, "seconds"
            numRows = inspect['numRows']
            numCols = inspect['numCols']
            missingValuesList = h2o_cmd.infoFromInspect(inspect, csvPathnamegz)
            print "\n" + csvPathnamegz, \
                "\n    numRows:", "{:,}".format(numRows), \
                "\n    numCols:", "{:,}".format(numCols)

            self.assertEqual(len(missingValuesList), 0, 
                "Don't expect any missing values. These cols had some: %s" % missingValuesList)
            # should match # of cols in header or ??
            self.assertEqual(inspect['numCols'], colCount,
                "parse created result with the wrong number of cols %s %s" % (inspect['numCols'], colCount))
            self.assertEqual(inspect['numRows'], rowCount,
                "parse created result with the wrong number of rows (header shouldn't count) %s %s" % \
                (inspect['numRows'], rowCount))

if __name__ == '__main__':
    h2o.unit_main()
