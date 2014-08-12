import unittest, random, sys, time, os
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts, h2o_browse as h2b, h2o_import as h2i, h2o_exec as h2e

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
        global SEED, localhost
        SEED = h2o.setup_random_seed()
        localhost = h2o.decide_if_localhost()
        java_extra_args='-XX:+PrintGCDetails'
        if (localhost):
            h2o.build_cloud(2, java_heap_GB=6, java_extra_args=java_extra_args)

        else:
            h2o_hosts.build_cloud_with_hosts(2, java_heap_GB=6, java_extra_args=java_extra_args)

    @classmethod
    def tearDownClass(cls):
        ## print "sleeping 3600"
        # h2o.sleep(3600)
        h2o.tear_down_cloud()

    def test_NOPASS_parse_500_cols_fvec(self):
        h2o.beta_features = True
        SYNDATASETS_DIR = h2o.make_syn_dir()
        tryList = [
            (100, 500, 'cA', 1800, 1800),
            ]

        h2b.browseTheCloud()
        for (rowCount, colCount, orig_hex_key, timeoutSecs, timeoutSecs2) in tryList:
            SEEDPERFILE = random.randint(0, sys.maxint)

            csvFilename = 'syn_' + str(SEEDPERFILE) + "_" + str(rowCount) + 'x' + str(colCount) + '.csv'
            csvPathname = SYNDATASETS_DIR + '/' + csvFilename

            # create sym links
            multifile = 100
            # there is already one file. assume it's the "0" case
            for p in range(1, multifile):
                csvPathnameLink = csvPathname + "_" + str(p)
                os.symlink(csvFilename, csvPathnameLink)

            print "\nCreating random", csvPathname
            write_syn_dataset(csvPathname, rowCount, colCount, SEEDPERFILE)

            for trial in range(5):
                hex_key = orig_hex_key + str(trial)
                start = time.time()
                parseResult = h2i.import_parse(path=csvPathname, schema='put', hex_key=hex_key, delete_on_done=1,
                    timeoutSecs=timeoutSecs, doSummary=False)
                print "Parse:", parseResult['destination_key'], "took", time.time() - start, "seconds"

                start = time.time()
                inspect = h2o_cmd.runInspect(None, parseResult['destination_key'], timeoutSecs=timeoutSecs2)
                print "Inspect:", parseResult['destination_key'], "took", time.time() - start, "seconds"
                h2o_cmd.infoFromInspect(inspect, csvPathname)
                print "\n" + csvPathname, \
                    "    numRows:", "{:,}".format(inspect['numRows']), \
                    "    numCols:", "{:,}".format(inspect['numCols'])

                # should match # of cols in header or ??
                self.assertEqual(inspect['numCols'], colCount,
                    "parse created result with the wrong number of cols %s %s" % (inspect['numCols'], colCount))
                self.assertEqual(inspect['numRows'], rowCount * multifile,
                    "parse created result with the wrong number of rows (header shouldn't count) %s %s" % \
                    (inspect['numRows'], rowCount * multifile))

                # h2i.delete_keys_at_all_nodes()

if __name__ == '__main__':
    h2o.unit_main()
