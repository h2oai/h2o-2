import unittest, random, sys, time
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i

# the shared exec expression creator and executor
import h2o_exec as h2e

def write_syn_dataset(csvPathname, rowCount, colCount, SEED):
    r1 = random.Random(SEED)
    dsf = open(csvPathname, "w+")

    for i in range(rowCount):
        rowData = []
        for j in range(colCount):
            # just all zeroes
            r = 0
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


    def test_many_cols(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        tryList = [
            (100, 10000, 'cI', 5),
            (100, 5000, 'cA', 5),
            (100, 6000, 'cB', 5),
            (100, 7000, 'cC', 5),
            (100, 8000, 'cD', 5),
            (100, 8200, 'cE', 5),
            (100, 8500, 'cF', 5),
            (100, 9000, 'cG', 5),
            (100, 11000, 'cH', 5),
            ]

        ### h2b.browseTheCloud()
        lenNodes = len(h2o.nodes)

        cnum = 0
        for (rowCount, colCount, hex_key, timeoutSecs) in tryList:
            cnum += 1
            csvFilename = 'syn_' + str(SEED) + "_" + str(rowCount) + 'x' + str(colCount) + '.csv'
            csvPathname = SYNDATASETS_DIR + '/' + csvFilename

            print "Creating random", csvPathname
            write_syn_dataset(csvPathname, rowCount, colCount, SEED)

            parseResult = h2i.import_parse(path=csvPathname, schema='put', hex_key=hex_key, 
                timeoutSecs=120, doSummary=False)
            print "Parse result['destination_key']:", parseResult['destination_key']

            # We should be able to see the parse result?
            inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
            print "\n" + csvFilename

            if not h2o.browse_disable:
                h2b.browseJsonHistoryAsUrlLastMatch("Inspect")
                time.sleep(5)

            # try new offset/view
            inspect = h2o_cmd.runInspect(None, parseResult['destination_key'], offset=100, view=100)
            inspect = h2o_cmd.runInspect(None, parseResult['destination_key'], offset=99, view=89)
            inspect = h2o_cmd.runInspect(None, parseResult['destination_key'], offset=-1, view=53)


if __name__ == '__main__':
    h2o.unit_main()
