import unittest, random, sys, time, os
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i, h2o_exec as h2e

def write_syn_dataset(csvPathname, rowCount, colCount, SEED):
    # 8 random generatators, 1 per column
    r1 = random.Random(SEED)
    dsf = open(csvPathname, "w+")

    for i in range(rowCount):
        rowData = []
        # just reuse the same col data, since we're just parsing
        # don't want to compress?
        # r = random.random()
        r = random.randint(1,1500)
        for j in range(colCount):
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
        java_extra_args='-XX:+PrintGCDetails'
        h2o.init(1, java_heap_GB=10, java_extra_args=java_extra_args)


    @classmethod
    def tearDownClass(cls):
        ## print "sleeping 3600"
        # h2o.sleep(3600)
        h2o.tear_down_cloud()

    def test_parse_500_cols_spill_fvec(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        tryList = [
            (100000, 500, 'cA', 500, 500),
            ]

        h2b.browseTheCloud()
        for (rowCount, colCount, orig_hex_key, timeoutSecs, timeoutSecs2) in tryList:
            SEEDPERFILE = random.randint(0, sys.maxint)

            csvFilename = 'syn_' + str(SEEDPERFILE) + "_" + str(rowCount) + 'x' + str(colCount) + '.csv'
            csvPathname = SYNDATASETS_DIR + '/' + csvFilename
            csvPathPattern = SYNDATASETS_DIR + '/' + '*syn*csv*'

            # create sym links
            # multifile = 100
            multifile = 50
            # there is already one file. assume it's the "0" case
            for p in range(1, multifile):
                csvPathnameLink = csvPathname + "_" + str(p)
                os.symlink(csvFilename, csvPathnameLink)

            print "\nCreating random", csvPathname
            write_syn_dataset(csvPathname, rowCount, colCount, SEEDPERFILE)

            # for trial in range(5):
            # try to pass with 2?
            for trial in range(2):
                hex_key = orig_hex_key + str(trial)
                start = time.time()
                parseResult = h2i.import_parse(path=csvPathPattern, hex_key=hex_key, delete_on_done=1,
                    timeoutSecs=timeoutSecs, retryDelaySecs=3, doSummary=False)
                print "Parse:", parseResult['destination_key'], "took", time.time() - start, "seconds"

                start = time.time()
                inspect = h2o_cmd.runInspect(None, parseResult['destination_key'], timeoutSecs=timeoutSecs2)
                print "Inspect:", parseResult['destination_key'], "took", time.time() - start, "seconds"
                h2o_cmd.infoFromInspect(inspect, csvPathname)
                print "\n" + csvPathname, \
                    "    numRows:", "{:,}".format(inspect['numRows']), \
                    "    numCols:", "{:,}".format(inspect['numCols']), \
                    "    byteSize:", "{:,}".format(inspect['byteSize'])

                # should match # of cols in header or ??
                self.assertEqual(inspect['numCols'], colCount,
                    "parse created result with the wrong number of cols %s %s" % (inspect['numCols'], colCount))
                self.assertEqual(inspect['numRows'], rowCount * multifile,
                    "parse created result with the wrong number of rows (header shouldn't count) %s %s" % \
                    (inspect['numRows'], rowCount * multifile))

                # h2i.delete_keys_at_all_nodes()

if __name__ == '__main__':
    h2o.unit_main()

# kevin@Kevin-Ubuntu4:~/h2o/py/testdir_single_jvm/sandbox/ice.W5qa_K/ice54321$ time ls -R * > /dev/null
# 
# real    0m6.900s
# user    0m6.260s
# sys    0m0.628s
# kevin@Kevin-Ubuntu4:~/h2o/py/testdir_single_jvm/sandbox/ice.W5qa_K/ice54321$ ls -R * | wc -l
# 651847
# 
# eventually you can hit os limits on # of files in a directory.
