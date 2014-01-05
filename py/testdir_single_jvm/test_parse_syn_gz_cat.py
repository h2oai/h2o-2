import unittest, random, sys, time
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts, h2o_browse as h2b, h2o_import as h2i, h2o_exec as h2e, h2o_util

print "Create csv with lots of same data (98% 0?), so gz will have high compression ratio"
print "Cat a bunch of them together, to get an effective large blow up inside h2o"
print "Can also copy the files to test multi-file gz parse...that will behave differently"
print "Behavior may be different depending on whether small ints are used, reals or used, or enums are used"
print "Remember the enum translation table has to be passed around between nodes, and updated atomically"

def write_syn_dataset(csvPathname, rowCount, colCount, SEED):
    # 8 random generatators, 1 per column
    r1 = random.Random(SEED)
    dsf = open(csvPathname, "w+")

    for i in range(rowCount):
        rowData = []
        for j in range(colCount):
            # r = h2o_util.choice_with_probability([(1.1, .02), (0.1, .98)])
            ones = .00001
            r = h2o_util.choice_with_probability([(1, ones), (0, 1-ones)])
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
        global SEED, localhost
        SEED = h2o.setup_random_seed()
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(1,java_heap_GB=4)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_parse_syn_gz_cat(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        tryList = [
            # summary fails with 100000 cols
            # overwrite the key each time to save space?
            (100, 40000, 'cF', 600),
            (100, 20000, 'cF', 600),
            (100, 10000, 'cF', 600),
            (100, 5000, 'cF', 600),
            ]

        # FILEREPL = 200
        FILEREPL = 2
        DOSUMMARY = True
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

            csvFilenameReplgz = csvFilename + "_" + str(FILEREPL) + "x.gz"
            csvPathnameReplgz = SYNDATASETS_DIR + '/' + csvFilenameReplgz

            start = time.time()
            print "Replicating", csvFilenamegz, "into", csvFilenameReplgz
            h2o_util.file_cat(csvPathnamegz, csvPathnamegz , csvPathnameReplgz)
            # no header? should we add a header? would have to be a separate gz?
            totalRows = 2 * rowCount
            for i in range(FILEREPL-2):
                h2o_util.file_append(csvPathnamegz, csvPathnameReplgz)
                totalRows += rowCount
            print "Replication took:", time.time() - start, "seconds"

            start = time.time()
            print "Parse start:", csvPathnameReplgz
            parseResult = h2i.import_parse(path=csvPathnameReplgz, schema='put', hex_key=hex_key, 
                timeoutSecs=timeoutSecs, doSummary=DOSUMMARY)
            print csvFilenameReplgz, 'parse time:', parseResult['response']['time']
            if DOSUMMARY:
                algo = "Parse and Summary:"
            else:
                algo = "Parse:"
            print algo , parseResult['destination_key'], "took", time.time() - start, "seconds"

            print "Inspecting.."
            start = time.time()
            inspect = h2o_cmd.runInspect(key=parseResult['destination_key'], timeoutSecs=timeoutSecs)
            print "Inspect:", parseResult['destination_key'], "took", time.time() - start, "seconds"
            num_rows = inspect['num_rows']
            num_cols = inspect['num_cols']
            value_size_bytes = inspect['value_size_bytes']
            h2o_cmd.infoFromInspect(inspect, csvPathnameReplgz)
            print "\n" + csvPathnameReplgz, \
                "\n    num_rows:", "{:,}".format(num_rows), \
                "\n    num_cols:", "{:,}".format(num_cols), \
                "\n    value_size_bytes:", "{:,}".format(value_size_bytes)

            # should match # of cols in header or ??
            self.assertEqual(inspect['num_cols'], colCount,
                "parse created result with the wrong number of cols %s %s" % (inspect['num_cols'], colCount))
            self.assertEqual(inspect['num_rows'], totalRows,
                "parse created result with the wrong number of rows (header shouldn't count) %s %s" % \
                (inspect['num_rows'], rowCount))

if __name__ == '__main__':
    h2o.unit_main()
