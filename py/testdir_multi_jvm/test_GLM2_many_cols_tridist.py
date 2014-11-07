import unittest, random, sys, time, os
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i, h2o_glm, h2o_exec as h2e

def write_syn_dataset(csvPathname, rowCount, colCount, SEED):
    # 8 random generatators, 1 per column
    r1 = random.Random(SEED)
    r2 = random.Random(SEED)
    dsf = open(csvPathname, "w+")

    for i in range(rowCount):
        rowData = []
        rowTotal = 0
        for j in range(colCount):
            ri1 = int(r1.triangular(0,2,1.5))
            rowData.append(ri1)

        result = r2.randint(0,1)
        rowData.append(str(result))
        # add the output twice, to try to match to it?
        # can't duplicate output in input, perfect separation, failure to converge
        ### rowData.append(str(result))
        ### print colCount, rowTotal, result
        rowDataCsv = ",".join(map(str,rowData))
        dsf.write(rowDataCsv + "\n")

    dsf.close()

paramDict = {
    'family': ['binomial'],
    'lambda': [1.0E-8],
    'alpha': [0.0],
    'max_iter': [50],
    'n_folds': [1],
    'beta_epsilon': [1.0E-4],
    }

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = h2o.setup_random_seed()
        h2o.init(2, java_heap_GB=7, use_flatfile=True)

    @classmethod
    def tearDownClass(cls):
        ### time.sleep(3600)
        h2o.tear_down_cloud()

    def test_GLM2_many_cols_tridist(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        tryList = [
            (10000,  10, 'cA', 300),
            (10000,  20, 'cB', 300),
            (10000,  30, 'cC', 300),
            (10000,  40, 'cD', 300),
            (10000,  50, 'cE', 300),
            (10000,  60, 'cF', 300),
            (10000,  70, 'cG', 300),
            (10000,  80, 'cH', 300),
            (10000,  90, 'cI', 300),
            (10000, 100, 'cJ', 300),
            (10000, 200, 'cK', 300),
            (10000, 300, 'cL', 300),
            (10000, 400, 'cM', 300),
            (10000, 500, 'cN', 300),
            (10000, 600, 'cO', 300),
            ]

        ### h2b.browseTheCloud()
        for (rowCount, colCount, hex_key, timeoutSecs) in tryList:
            SEEDPERFILE = random.randint(0, sys.maxint)
            csvFilename = 'syn_' + str(SEEDPERFILE) + "_" + str(rowCount) + 'x' + str(colCount) + '.csv'
            csvPathname = SYNDATASETS_DIR + '/' + csvFilename

            print "\nCreating random", csvPathname
            write_syn_dataset(csvPathname, rowCount, colCount, SEEDPERFILE)

            parseResult = h2i.import_parse(path=csvPathname, schema='put', hex_key=hex_key, timeoutSecs=30)
            print "\nParse result['destination_key']:", parseResult['destination_key']

            # We should be able to see the parse result?
            inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
            print "\n" + csvFilename

            paramDict2 = {}
            for k in paramDict:
                paramDict2[k] = paramDict[k][0]

            y = colCount
            kwargs = {'response': y}
            kwargs.update(paramDict2)

            start = time.time()
            glm = h2o_cmd.runGLM(parseResult=parseResult, timeoutSecs=timeoutSecs, **kwargs)
            print "glm end on ", csvPathname, 'took', time.time() - start, 'seconds'
            h2o_glm.simpleCheckGLM(self, glm, 'C9', **kwargs)

            if not h2o.browse_disable:
                h2b.browseJsonHistoryAsUrlLastMatch("Inspect")
                time.sleep(5)


if __name__ == '__main__':
    h2o.unit_main()
