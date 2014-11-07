import unittest, random, sys, time, re
sys.path.extend(['.','..','../..','py'])

import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i, h2o_glm

def write_syn_dataset(csvPathname, rowCount, colCount, SEED):
    # FIX! all this fanciness shouldn't be needed. GLM shouldn't be able to learn
    # the single RNG

    # getting correlated results?
    r1 = random.Random(SEED)
    ### r1.jumpahead(922377089)   

    r2 = random.Random(SEED)
    ### r2.jumpahead(488915466)
    dsf = open(csvPathname, "w+")

    for i in range(rowCount):
        rowData = []
        rowTotal = 0
        for j in range(colCount):
            ri1 = r1.randint(0,1)
            rowData.append(ri1)

        result = r2.randint(0,1)
        rowData.append(str(result))
        ### print colCount, rowTotal, result
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
        h2o.init(1,use_flatfile=True)

    @classmethod
    def tearDownClass(cls):
        ### time.sleep(3600)
        h2o.tear_down_cloud()

    def test_GLM2_convergence_1(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        tryList = [
            (100, 50,  'cD', 300),
            (100, 100, 'cE', 300),
            (100, 200, 'cF', 300),
            (100, 300, 'cG', 300),
            (100, 400, 'cH', 300),
            (100, 500, 'cI', 300),
        ]

        ### h2b.browseTheCloud()
        lenNodes = len(h2o.nodes)

        for (rowCount, colCount, hex_key, timeoutSecs) in tryList:
            SEEDPERFILE = random.randint(0, sys.maxint)
            csvFilename = 'syn_%s_%sx%s.csv' % (SEEDPERFILE,rowCount,colCount)
            csvPathname = SYNDATASETS_DIR + '/' + csvFilename
            print "\nCreating random", csvPathname
            write_syn_dataset(csvPathname, rowCount, colCount, SEEDPERFILE)

            parseResult = h2i.import_parse(path=csvPathname, hex_key=hex_key, timeoutSecs=10, schema='put')
            print "Parse result['destination_key']:", parseResult['destination_key']
            inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
            print "\n" + csvFilename

            y = colCount
            kwargs = {
                    'max_iter': 10, 
                    'lambda': 1e-8,
                    'alpha': 0,
                    'n_folds': 0,
                    'beta_epsilon': 1e-4,
                    }

            kwargs['response'] = y
            emsg = None
            # FIX! how much should we loop here. 
            for i in range(3):
                start = time.time()
                glm = h2o_cmd.runGLM(parseResult=parseResult, timeoutSecs=timeoutSecs, **kwargs)
                print 'glm #', i, 'end on', csvPathname, 'took', time.time() - start, 'seconds'
                # we can pass the warning, without stopping in the test, so we can 
                # redo it in the browser for comparison
                (warnings, coefficients, intercept) = h2o_glm.simpleCheckGLM(self, 
                    glm, None, **kwargs)

            # gets the failed to converge, here, after we see it in the browser too
            if emsg is not None:
                raise Exception(emsg)

if __name__ == '__main__':
    h2o.unit_main()
