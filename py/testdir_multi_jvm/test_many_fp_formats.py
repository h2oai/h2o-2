import unittest, random, sys, time, os
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i, h2o_exec as h2e
import h2o_util

def write_syn_dataset(csvPathname, rowCount, colCount, SEEDPERFILE, sel):
    # we can do all sorts of methods off the r object
    r = random.Random(SEEDPERFILE)

    ## MIN = -1e20
    ## MAX = 1e20
    # okay to use the same value across the whole dataset?
    ## val = r.uniform(MIN,MAX)
    val = r.triangular(-1e9,1e9,0)
    valFormatted = h2o_util.fp_format(val, sel)

    dsf = open(csvPathname, "w+")
    for i in range(rowCount):
        rowData = []
        for j in range(colCount):
            rowData.append(valFormatted) # f should always return string
        rowDataCsv = ",".join(rowData)
        dsf.write(rowDataCsv + "\n")
    dsf.close()

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = h2o.setup_random_seed()
        h2o.init(2,java_heap_GB=5)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_many_fp_formats(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        tryList = [
            # (100, 100, 'cB', 180),
            (100000, 10, 'cA', 180),
            # (100, 900, 'cC', 30),
            # (100, 500, 'cD', 30),
            # (100, 100, 'cE', 30),
            ]
        
        for (rowCount, colCount, hex_key, timeoutSecs) in tryList:
            NUM_CASES = h2o_util.fp_format()
            print "Will do %s" % NUM_CASES
            for sel in range(NUM_CASES): # len(caseList)
                SEEDPERFILE = random.randint(0, sys.maxint)
                csvFilename = "syn_%s_%s_%s_%s.csv" % (SEEDPERFILE, sel, rowCount, colCount)
                csvPathname = SYNDATASETS_DIR + '/' + csvFilename

                print "Creating random", csvPathname
                write_syn_dataset(csvPathname, rowCount, colCount, SEEDPERFILE, sel)

                hex_key = hex_key + "_" + str(sel)
                parseResult = h2i.import_parse(path=csvPathname, schema='put', hex_key=hex_key,
                    timeoutSecs=timeoutSecs, doSummary=False)
                h2o_cmd.runSummary(key=parseResult['destination_key'], max_qbins=100)

                print "Parse result['destination_key']:", hex_key
                inspect = h2o_cmd.runInspect(None, hex_key)
                print "Removing", hex_key
                h2o.nodes[0].remove_key(hex_key)

                # if not h2o.browse_disable:
                #     h2b.browseJsonHistoryAsUrlLastMatch("Inspect")
                #     time.sleep(3)

if __name__ == '__main__':
    h2o.unit_main()
