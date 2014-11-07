import unittest, time, sys, random, math, getpass
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_import as h2i, h2o_util, h2o_print as h2p

ROWS = 10000 # passes
COLS = 4
NA_ROW_RATIO = 1

def write_syn_dataset(csvPathname, rowCount, colCount, expectedMin, expectedMax, SEED):
    r1 = random.Random(SEED)
    dsf = open(csvPathname, "w+")

    expectedRange = (expectedMax - expectedMin)
    for i in range(rowCount):
        rowData = []
        ri = expectedMin + (random.uniform(0,1) * expectedRange)
        for j in range(colCount):
            rowData.append(ri)
        rowDataCsv = ",".join(map(str,rowData))
        dsf.write(rowDataCsv + "\n")

        # add 5 rows of NAs, for every row of valid dat
        rowData = []
        for j in range(colCount):
            # this shouldn't be h2o parsed as an enum?...NA is special?
            rowData.append(',,')
        rowDataCsv = ",".join(map(str,rowData))
        for k in range(NA_ROW_RATIO):
            dsf.write(rowDataCsv + "\n")

    dsf.close()

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = h2o.setup_random_seed()
        h2o.init()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_anomaly_uniform_w_NA(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        tryList = [
            # colname, (min, 25th, 50th, 75th, max)
            (ROWS, COLS, 'x.hex', 1, 20000),
            (ROWS, COLS, 'x.hex', -5000, 0),
            (ROWS, COLS, 'x.hex', -100000, 100000),
            (ROWS, COLS, 'x.hex', -1, 1),

            (ROWS, COLS, 'A.hex', 1, 100),
            (ROWS, COLS, 'A.hex', -99, 99),

            (ROWS, COLS, 'B.hex', 1, 10000),
            (ROWS, COLS, 'B.hex', -100, 100),

            (ROWS, COLS, 'C.hex', 1, 100000),
            (ROWS, COLS, 'C.hex', -101, 101),
        ]

        trial = 1
        x = 0
        timeoutSecs = 60
        for (rowCount, colCount, hex_key, expectedMin, expectedMax) in tryList:
            SEEDPERFILE = random.randint(0, sys.maxint)
            x += 1

            csvFilename = 'syn_' + "binary" + "_" + str(rowCount) + 'x' + str(colCount) + '.csv'
            csvPathname = SYNDATASETS_DIR + '/' + csvFilename
            csvPathnameFull = h2i.find_folder_and_filename(None, csvPathname, returnFullPath=True)

            print "Creating random", csvPathname
            write_syn_dataset(csvPathname, rowCount, colCount, expectedMin, expectedMax, SEEDPERFILE)
            parseResult = h2i.import_parse(path=csvPathname, schema='put', 
                hex_key=hex_key, timeoutSecs=30, doSummary=False)
            print "Parse result['destination_key']:", parseResult['destination_key']
            inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
            numRows = inspect["numRows"]
            numCols = inspect["numCols"]
            print "numRows:", numRows, "numCols:", numCols

            model_key = "m.hex"
            kwargs = {
                'ignored_cols'                 : None,
                'response'                     : numCols-1,
                'classification'               : 0,
                'activation'                   : 'RectifierWithDropout',
                'input_dropout_ratio'          : 0.2,
                'hidden'                       : '117',
                'adaptive_rate'                : 0,
                'rate'                         : 0.005,
                'rate_annealing'               : 1e-6,
                'momentum_start'               : 0.5,
                'momentum_ramp'                : 100000,
                'momentum_stable'              : 0.9,
                'l1'                           : 0.00001,
                'l2'                           : 0.0000001,
                'seed'                         : 98037452452,
                # 'loss'                         : 'CrossEntropy',
                'max_w2'                       : 15,
                'initial_weight_distribution'  : 'UniformAdaptive',
                #'initial_weight_scale'         : 0.01,
                'epochs'                       : 2.0,
                'destination_key'              : model_key,
                # 'validation'                   : None,
                'score_interval'               : 10000,
                'autoencoder'                  : 1,
                }

            timeoutSecs = 600
            start = time.time()
            nn = h2o_cmd.runDeepLearning(parseResult=parseResult, timeoutSecs=timeoutSecs, **kwargs)
            print "neural net end. took", time.time() - start, "seconds"


            kwargs = {
                'destination_key': "a.hex",
                'source': parseResult['destination_key'],
                'dl_autoencoder_model': model_key,
                'thresh': 1.0
            }

            anomaly = h2o.nodes[0].anomaly(timeoutSecs=30, **kwargs)
            inspect = h2o_cmd.runInspect(None, "a.hex")
            numRows = inspect["numRows"]
            numCols = inspect["numCols"]
            print "anomaly: numRows:", numRows, "numCols:", numCols
            self.assertEqual(numCols,1)
            # twice as many rows because of NA injection
            self.assertEqual(numRows,rowCount*(1 + NA_ROW_RATIO))

            # first col has the anomaly info. other cols are the same as orig data
            aSummary = h2o_cmd.runSummary(key='a.hex', cols=0)
            h2o_cmd.infoFromSummary(aSummary)


            print "anomaly:", h2o.dump_json(anomaly)
            trial += 1
            h2i.delete_keys_at_all_nodes()


if __name__ == '__main__':
    h2o.unit_main()

