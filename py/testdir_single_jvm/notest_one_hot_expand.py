import unittest, random, sys, time, os
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i
import h2o_exec as h2e

def write_syn_dataset(csvPathname, rowCount, colCount, SEED):
    r1 = random.Random(SEED)
    dsf = open(csvPathname, "w+")

    for i in range(rowCount):
        rowData = []
        for j in range(colCount):
            r = random.choice(['a','b','c','d'])
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


    def test_one_hot_expand_fvec(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        tryList = [
            (100, 1100, 'cA', 5),
            (100, 1000, 'cB', 5),
            (100, 900, 'cC', 5),
            (100, 800, 'cD', 5),
            (100, 700, 'cE', 5),
            (100, 600, 'cF', 5),
            (100, 500, 'cG', 5),
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

            parseResult = h2i.import_parse(path=csvPathname, schema='put', hex_key=hex_key, timeoutSecs=10)

            inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
            h2o_cmd.infoFromInspect(inspect, csvPathname)

            # does it modify the original or ?
            oneHotResult = h2o.nodes[0].one_hot(source=parseResult['destination_key'])

            inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
            h2o_cmd.infoFromInspect(inspect, csvPathname)


if __name__ == '__main__':
    h2o.unit_main()
