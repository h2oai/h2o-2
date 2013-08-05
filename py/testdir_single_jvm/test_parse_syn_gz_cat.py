import unittest, random, sys, time
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts, h2o_browse as h2b, h2o_import as h2i, h2o_exec as h2e, h2o_util

print "Create csv with lots of same data (95% 0?), so gz will have high compression ratio"
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
            r = h2o_util.choice_with_probability([(1.1, .05), (0.1, .95)])
            rowData.append(r)

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
            h2o.build_cloud(1,java_heap_GB=14)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_parse_syn_gz_cat(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        tryList = [
            # summary fails with 100000 cols
            (10, 5000, 'cE', 600),
            (10, 10000, 'cF', 600),
            (10, 50000, 'cF', 600),
            ]

        FILEREPL = 200
        DOSUMMARY = True
        # h2b.browseTheCloud()
        for (rowCount, colCount, key2, timeoutSecs) in tryList:
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
            parseKey = h2o_cmd.parseFile(None, csvPathnameReplgz, key2=key2, timeoutSecs=timeoutSecs, doSummary=DOSUMMARY)
            print csvFilenameReplgz, 'parse time:', parseKey['response']['time']
            if DOSUMMARY:
                algo = "Parse and Summary:"
            else:
                algo = "Parse:"
            print algo , parseKey['destination_key'], "took", time.time() - start, "seconds"

            print "Inspecting.."
            start = time.time()
            inspect = h2o_cmd.runInspect(None, parseKey['destination_key'], timeoutSecs=timeoutSecs)
            print "Inspect:", parseKey['destination_key'], "took", time.time() - start, "seconds"
            h2o_cmd.infoFromInspect(inspect, csvPathname)
            print "\n" + csvPathname, \
                "    num_rows:", "{:,}".format(inspect['num_rows']), \
                "    num_cols:", "{:,}".format(inspect['num_cols'])

            # should match # of cols in header or ??
            self.assertEqual(inspect['num_cols'], colCount,
                "parse created result with the wrong number of cols %s %s" % (inspect['num_cols'], colCount))
            self.assertEqual(inspect['num_rows'], totalRows,
                "parse created result with the wrong number of rows (header shouldn't count) %s %s" % \
                (inspect['num_rows'], rowCount))

if __name__ == '__main__':
    h2o.unit_main()
