import unittest, random, sys, time
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i, h2o_glm

def write_syn_dataset(csvPathname, rowCount, colCount, SEED):
    r1 = random.Random(SEED)
    dsf = open(csvPathname, "w+")

    for i in range(rowCount):
        rowData = []
        for j in range(colCount):
            ri = r1.randint(0,1)
            rowData.append(ri)

        ri = r1.randint(0,1)
        rowData.append(ri)

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
        h2o.init(1,java_heap_GB=10)

    @classmethod
    def tearDownClass(cls):
        ### time.sleep(3600)
        h2o.tear_down_cloud()

    def test_GLM2_many_cols(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()

        tryList = [
            # (2, 100, 'cA', 300), 
            # (4, 200, 'cA', 300), 
            (10000, 1000, 'cB', 300), 
            (10000, 3000, 'cC', 500), 
            ]

        ### h2b.browseTheCloud()
        lenNodes = len(h2o.nodes)

        for (rowCount, colCount, hex_key, timeoutSecs) in tryList:
            SEEDPERFILE = random.randint(0, sys.maxint)
            # csvFilename = 'syn_' + str(SEEDPERFILE) + "_" + str(rowCount) + 'x' + str(colCount) + '.csv'
            csvFilename = 'syn_' + "binary" + "_" + str(rowCount) + 'x' + str(colCount) + '.csv'
            csvPathname = SYNDATASETS_DIR + '/' + csvFilename

            print "Creating random", csvPathname
            write_syn_dataset(csvPathname, rowCount, colCount, SEEDPERFILE)

            parseResult = h2i.import_parse(path=csvPathname, hex_key=hex_key, schema='put', timeoutSecs=90)
            print "Parse result['destination_key']:", parseResult['destination_key']

            # We should be able to see the parse result?
            inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
            print "\n" + csvFilename

            y = colCount
            # normally we dno't create x and rely on the default
            # create the big concat'ed x like the browser, to see what happens
            # x = ','.join(map(str, range(colCount)))
            kwargs = {
                'response': 'C' + str(y), 
                'max_iter': 10, 
                'n_folds': 1, 
                'alpha': 0.0, 
                'lambda': 0.0,
            }

            start = time.time()
            x = h2o_glm.goodXFromColumnInfo(y, key=parseResult['destination_key'], timeoutSecs=300, returnStringX=False)
            # all-zero/all-na cols are dropped. figure out expected # of coefficients

            glm = h2o_cmd.runGLM(parseResult=parseResult, timeoutSecs=timeoutSecs, **kwargs)
            print "glm end on ", csvPathname, 'took', time.time() - start, 'seconds'
            h2o_glm.simpleCheckGLM(self, glm, None, **kwargs)
            expectedCoeffNum = len(x)

            # check that the number of entries in coefficients is right (intercept is in there)
            actualCoeffNum = len(glm['glm_model']['submodels'][0]['beta']) - 1
            if actualCoeffNum!=expectedCoeffNum:
                raise Exception("Should be %s expected coefficients in result." % expectedCoeffNum)


if __name__ == '__main__':
    h2o.unit_main()
