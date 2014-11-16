import unittest, random, sys, time
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i, h2o_glm

def write_syn_dataset(csvPathname, rowCount, colCount, SEED):
    # 8 random generatators, 1 per column
    r1 = random.Random(SEED)
    dsf = open(csvPathname, "w+")

    for i in range(rowCount):
        rowData = []
        rowTotal = 0
        # having reals makes it less likely to fail to converge?
        for j in range(colCount):
            ri1 = int(r1.gauss(1,.1))
            rowTotal += ri1
            rowData.append(ri1 + 0.1) # odd bias shift

        # sum the row, and make output 1 if > (5 * rowCount)
        if (rowTotal > (0.5 * colCount)): 
            result = 1
        else:
            result = 0

        rowData.append(result)
        ### print colCount, rowTotal, result
        rowDataCsv = ",".join(map(str,rowData))
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

    def test_GLM2_many_cols_real(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        tryList = [
            (100, 1000, 'cA', 100),
            (100, 3000, 'cB', 300),
            ]

        ### h2b.browseTheCloud()
        for (rowCount, colCount, hex_key, timeoutSecs) in tryList:
            SEEDPERFILE = random.randint(0, sys.maxint)
            csvFilename = 'syn_' + str(SEEDPERFILE) + "_" + str(rowCount) + 'x' + str(colCount) + '.csv'
            csvPathname = SYNDATASETS_DIR + '/' + csvFilename

            print "Creating random", csvPathname
            write_syn_dataset(csvPathname, rowCount, colCount, SEEDPERFILE)

            start = time.time()
            parseResult = h2i.import_parse(path=csvPathname, schema='put', hex_key=hex_key, timeoutSecs=60)
            elapsed = time.time() - start
            print "Parse result['destination_key']:", parseResult['destination_key']

            algo = "Parse"
            l = '{:d} jvms, {:d}GB heap, {:s} {:s} {:6.2f} secs'.format(
                len(h2o.nodes), tryHeap, algo, csvFilename, elapsed)
            print l
            h2o.cloudPerfH2O.message(l)

            # We should be able to see the parse result?
            inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
            print "\n" + csvFilename

            y = colCount
            # just limit to 2 iterations..assume it scales with more iterations
            kwargs = {
                'response': y,
                'max_iter': 2, 
                'family': 'binomial',
                'lambda': 1.e-4,
                'alpha': 0.6,
                'n_folds': 1,
                'beta_epsilon': 1.e-4,
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
                len(h2o.nodes), tryHeap, algo, csvFilename, elapsed)
            print l
            h2o.cloudPerfH2O.message(l)

if __name__ == '__main__':
    h2o.unit_main()
