import unittest, random, sys, time, os
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i
import h2o_exec as h2e

def write_syn_dataset(csvPathname, rowCount, colCount, header, SEED):
    r1 = random.Random(SEED)
    dsf = open(csvPathname, "w+")


    for i in range(rowCount):
        rowData = []
        for j in range(colCount):
            # header names need to be unique
            if header and i==0:
                r = "a" + str(j)
            else:
                r = "a"
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
        h2o.init(1,java_heap_GB=1)


    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()


    def test_many_cols_enum(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        tryList = [
            (100, 11000, 0, 'cA', 180),
            (100, 10000, 1, 'cB', 180),
            (100, 9000, 0, 'cC', 180),
            (100, 8000, 1, 'cD', 180),
            (100, 7000, 0, 'cE', 180),
            (100, 6000, 1, 'cF', 180),
            (100, 5000, 0, 'cG', 180),
            ]

        ### h2b.browseTheCloud()
        lenNodes = len(h2o.nodes)

        cnum = 0
        # it's interesting to force the first enum row to be used as header or not
        # with many cols, we tend to hit limits about stuff fitting in a chunk (header or data)
        for (rowCount, colCount, header, hex_key, timeoutSecs) in tryList:
            cnum += 1
            csvFilename = 'syn_' + str(SEED) + "_" + str(rowCount) + 'x' + str(colCount) + '.csv'
            csvPathname = SYNDATASETS_DIR + '/' + csvFilename

            print "Creating random", csvPathname
            write_syn_dataset(csvPathname, rowCount, colCount, header, SEED)

            parseResult = h2i.import_parse(path=csvPathname, schema='put', header=header, 
                hex_key=hex_key, timeoutSecs=60)
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
