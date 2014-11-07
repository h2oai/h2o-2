import unittest, random, sys, time
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i, h2o_glm

def write_syn_libsvm_dataset(csvPathname, rowCount, colCount, SEED):
    r1 = random.Random(SEED)
    dsf = open(csvPathname, "w+")

    for i in range(rowCount):
        rowData = []
        for j in range(colCount):
            ri = r1.randint(0,1)
            if ri!=0: # don't include 0's
                colNumber = j + 1
                rowData.append(str(colNumber) + ":" + str(ri))

        ri = r1.randint(0,1)
        # output class goes first
        rowData.insert(0, str(ri))
        rowDataCsv = " ".join(rowData) # already all strings
        dsf.write(rowDataCsv + "\n")

    dsf.close()

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = h2o.setup_random_seed()
        h2o.init(1,java_heap_GB=10)

    @classmethod
    def tearDownClass(cls):
        ### time.sleep(3600)
        h2o.tear_down_cloud()

    def test_GLM2_many_cols_libsvm(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()

        tryList = [
            (100,  3000, 'cA', 300), 
            (100,  5000, 'cB', 500), 
            # too slow!
            # (100, 10000, 'cC', 800), 
            ]

        ### h2b.browseTheCloud()
        lenNodes = len(h2o.nodes)

        for (rowCount, colCount, hex_key, timeoutSecs) in tryList:
            SEEDPERFILE = random.randint(0, sys.maxint)
            csvFilename = 'syn_' + "binary" + "_" + str(rowCount) + 'x' + str(colCount) + '.svm'
            csvPathname = SYNDATASETS_DIR + '/' + csvFilename

            print "Creating random libsvm:", csvPathname
            write_syn_libsvm_dataset(csvPathname, rowCount, colCount, SEEDPERFILE)

            parseResult = h2i.import_parse(path=csvPathname, hex_key=hex_key, schema='put', timeoutSecs=timeoutSecs)
            print "Parse result['destination_key']:", parseResult['destination_key']

            # We should be able to see the parse result?
            inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
            print "\n" + csvFilename

            y = colCount
            kwargs = {'response': y, 'max_iter': 2, 'n_folds': 1, 'alpha': 0.2, 'lambda': 1e-5}

            start = time.time()
            glm = h2o_cmd.runGLM(parseResult=parseResult, timeoutSecs=timeoutSecs, **kwargs)
            print "glm end on ", csvPathname, 'took', time.time() - start, 'seconds'
            h2o_glm.simpleCheckGLM(self, glm, None, **kwargs)


if __name__ == '__main__':
    h2o.unit_main()
