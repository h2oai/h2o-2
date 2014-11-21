import unittest, random, sys, time, getpass
sys.path.extend(['.','..','../..','py'])

# FIX! add cases with shuffled data!
import h2o, h2o_cmd, h2o_glm
import h2o_browse as h2b, h2o_import as h2i, h2o_exec as h2e

def write_syn_dataset(csvPathname, rowCount, colCount, SEED, translateList):
    # do we need more than one random generator?
    r1 = random.Random(SEED)
    dsf = open(csvPathname, "w+")

    for i in range(rowCount):
        rowData = []
        for j in range(colCount):
            ### ri1 = int(r1.triangular(0,2,1.5))
            ri1 = int(r1.triangular(1,5,2.5))
            rowData.append(ri1)

        rowTotal = sum(rowData)
        ### print rowData
        if translateList is not None:
            for i, iNum in enumerate(rowData):
                # numbers should be 1-5, mapping to a-d
                rowData[i] = translateList[iNum-1]

        rowAvg = (rowTotal + 0.0)/colCount
        ### print rowAvg
        # if rowAvg > 2.25:
        if rowAvg > 2.3:
            result = 1
        else:
            result = 0
        ### print colCount, rowTotal, result
        rowDataStr = map(str,rowData)
        rowDataStr.append(str(result))
        rowDataCsv = ",".join(rowDataStr)
        dsf.write(rowDataCsv + "\n")

    dsf.close()


class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED, tryHeap
        tryHeap = 14
        SEED = h2o.setup_random_seed()
        h2o.init(1, enable_benchmark_log=True, java_heap_GB=tryHeap)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_GLM2_many_cols_enum(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        translateList = ['a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u']

        if getpass.getuser() == 'kevin': # longer run
            tryList = [
                (10000,  100, 'cA', 100),
                (10000,  300, 'cB', 300),
                (10000,  500, 'cC', 700),
                (10000,  700, 'cD', 3600),
                (10000,  900, 'cE', 3600),
                (10000,  1000, 'cF', 3600),
                (10000,  1300, 'cG', 3600),
                (10000,  1700, 'cH', 3600),
                (10000,  2000, 'cI', 3600),
                (10000,  2500, 'cJ', 3600),
                (10000,  3000, 'cK', 3600),
                ]
        else:
            tryList = [
                (10000,  100, 'cA', 100),
                (10000,  300, 'cC', 300),
            ]

        ### h2b.browseTheCloud()

        for (rowCount, colCount, hex_key, timeoutSecs) in tryList:
            SEEDPERFILE = random.randint(0, sys.maxint)
            csvFilename = 'syn_' + str(SEEDPERFILE) + "_" + str(rowCount) + 'x' + str(colCount) + '.csv'
            csvPathname = SYNDATASETS_DIR + '/' + csvFilename

            print "\nCreating random", csvPathname
            write_syn_dataset(csvPathname, rowCount, colCount, SEEDPERFILE, translateList)

            start = time.time()
            parseResult = h2i.import_parse(path=csvPathname, hex_key=hex_key, schema='put', timeoutSecs=30)
            elapsed = time.time() - start

            print "Parse result['destination_key']:", parseResult['destination_key']

            algo = "Parse"
            l = '{:d} jvms, {:d}GB heap, {:s} {:s} {:6.2f} secs'.format(
                len(h2o.nodes), h2o.nodes[0].java_heap_GB, algo, csvFilename, elapsed)
            print l
            h2o.cloudPerfH2O.message(l)

            # We should be able to see the parse result?
            inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
            print "\n" + csvFilename

            y = colCount
            # just limit to 2 iterations..assume it scales with more iterations
            # java.lang.IllegalArgumentException: Too many predictors! 
            # GLM can only handle 5000 predictors, got 5100, try to run with strong_rules enabled.

            kwargs = {
                'response': y, 
                'max_iter': 2, 
                'family': 'binomial',
                'lambda': 1e-4,
                'alpha': 0.6,
                'n_folds': 1,
                'beta_epsilon': 1e-4,
                'strong_rules': 1,
            }

            start = time.time()
            glm = h2o_cmd.runGLM(parseResult=parseResult, timeoutSecs=timeoutSecs, **kwargs)
            elapsed = time.time() - start

            h2o.check_sandbox_for_errors()
            print "glm end on ", csvPathname, 'took', elapsed, 'seconds', \
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)
            h2o_glm.simpleCheckGLM(self, glm, None, **kwargs)

            algo = "GLM " 
            l = '{:d} jvms, {:d}GB heap, {:s} {:s} {:6.2f} secs'.format(
                len(h2o.nodes), h2o.nodes[0].java_heap_GB, algo, csvFilename, elapsed)
            print l
            h2o.cloudPerfH2O.message(l)


if __name__ == '__main__':
    h2o.unit_main()
