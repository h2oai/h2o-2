import unittest, random, sys, time
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i
import h2o_kmeans, h2o_exec as h2e

def write_syn_dataset(csvPathname, rowCount, colCount, SEED):
    r1 = random.Random(SEED)
    dsf = open(csvPathname, "w+")

    for i in range(rowCount):
        rowData = []
        for j in range(colCount):
            r = random.choice(['a', 'b', 'c', 'd'])
            rowData.append(r)

        r = random.randint(0,2)
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
        h2o.init(1,java_heap_GB=4)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()


    def test_KMeans2_enum(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        tryList = [
            (100, 11, 'cA', 10),
            (100, 10, 'cB', 10),
            (100, 9, 'cC', 10),
            (100, 8, 'cD', 10),
            (100, 7, 'cE', 10),
            (100, 6, 'cF', 10),
            (100, 5, 'cG', 10),
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
            parseResult = h2i.import_parse(path=csvPathname, schema='put', hex_key=csvFilename + ".hex")
            print "Parse result['destination_key']:", parseResult['destination_key']

            kwargs = {
                'k': 2, 
                'initialization': 'Furthest', 
                'destination_key': 'benign_k.hex',
                'max_iter': 10,
            }
            kmeans = h2o_cmd.runKMeans(parseResult=parseResult, timeoutSecs=timeoutSecs, **kwargs)
            h2o_kmeans.bigCheckResults(self, kmeans, csvPathname, parseResult, 'd', **kwargs)

if __name__ == '__main__':
    h2o.unit_main()
