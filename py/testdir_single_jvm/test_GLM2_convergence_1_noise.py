import unittest, random, sys, time, re
sys.path.extend(['.','..','../..','py'])

import h2o, h2o_cmd, h2o_browse as h2b, h2o_glm, h2o_import as h2i

def write_syn_dataset(csvPathname, rowCount, colCount, SEED):
    # FIX! all this fanciness shouldn't be needed. GLM shouldn't be able to learn
    # the single RNG

    # getting correlated results?
    r1 = random.Random(SEED)
    # keep a single thread from the original SEED, for repeatability.
    SEED2 = r1.randint(0, sys.maxint)
    r2 = random.Random(SEED2)
    SEED3 = r1.randint(0, sys.maxint)
    r3 = random.Random(SEED3)
    dsf = open(csvPathname, "w+")

    for i in range(rowCount):
        rowData = []
        rowTotal = 0
        # do jumpahead per row, so the combination of rows plus col dice rolls
        # doesn't allow prediction of the RNG so well? (an issue with 500 col datasets)
        ### r1.jumpahead(922377089)   
        ### r2.jumpahead(488915466)
        ### r3.jumpahead(743976213)

        # use r3 to randomly inject 5% noise. noise is complement
        for j in range(colCount):
            ri1 = r1.randint(0,1)
            ri3 = r3.randint(0,1)
            rs = (ri1 + ri3) % 2
            rowData.append(rs)

        # use r3 to randomly inject 5% noise. noise is complement
        ri2 = r2.randint(0,1)
        ri3 = r3.randint(0,1)
        result = (ri2 + ri3) % 2
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
        h2o.init(use_flatfile=True)

    @classmethod
    def tearDownClass(cls):
        ### time.sleep(3600)
        h2o.tear_down_cloud()

    def test_GLM2_convergence_1_noise(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        tryList = [
            (10000, 50,  'cD', 300),
            (10000, 100, 'cE', 300),
            (10000, 200, 'cF', 300),
            (10000, 300, 'cG', 300),
            (10000, 400, 'cH', 300),
            (10000, 500, 'cI', 300),
        ]

        ### h2b.browseTheCloud()
        lenNodes = len(h2o.nodes)

        USEKNOWNFAILURE = False
        for (rowCount, colCount, hex_key, timeoutSecs) in tryList:
            SEEDPERFILE = random.randint(0, sys.maxint)
            csvFilename = 'syn_%s_%sx%s.csv' % (SEEDPERFILE,rowCount,colCount)
            csvPathname = SYNDATASETS_DIR + '/' + csvFilename
            print "\nCreating random", csvPathname
            write_syn_dataset(csvPathname, rowCount, colCount, SEEDPERFILE)

            if USEKNOWNFAILURE:
                csvFilename = 'failtoconverge_100x50.csv'
                csvPathname = 'logreg/' + csvFilename

            parseResult = h2i.import_parse(path=csvPathname, hex_key=hex_key, timeoutSecs=10, schema='put')
            print "Parse result['destination_key']:", parseResult['destination_key']
            inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
            print "\n" + csvFilename

            y = colCount
            kwargs = {
                'max_iter': 40, 
                'lambda': 1e-4,
                'alpha': 0,
                'n_folds': 2,
                'beta_epsilon': 1e-4,
                }

            if USEKNOWNFAILURE:
                kwargs['response'] = 50
            else:
                kwargs['response'] = y

            emsg = None
            for i in range(1):
                start = time.time()
                glm = h2o_cmd.runGLM(parseResult=parseResult, timeoutSecs=timeoutSecs, **kwargs)
                print 'glm #', i, 'end on', csvPathname, 'took', time.time() - start, 'seconds'
                # we can pass the warning, without stopping in the test, so we can 
                # redo it in the browser for comparison
                (warnings, coefficients, intercept) = h2o_glm.simpleCheckGLM(self, 
                    glm, None, allowFailWarning=True, **kwargs)

                if 1==0:
                    print "\n", "\ncoefficients in col order:"
                    # since we're loading the x50 file all the time..the real colCount 
                    # should be 50 (0 to 49)
                    if USEKNOWNFAILURE:
                        showCols = 50
                    else:
                        showCols = colCount
                    for c in range(showCols):
                        print "%s:\t%.6e" % (c, coefficients[c])
                    print "intercept:\t %.6e" % intercept

                # gets the failed to converge, here, after we see it in the browser too
                x = re.compile("[Ff]ailed")
                if warnings:
                    for w in warnings:
                        if (re.search(x,w)): 
                            # first
                            if emsg is None: emsg = w
                            print w
                if emsg: break
        
            if not h2o.browse_disable:
                h2b.browseJsonHistoryAsUrlLastMatch("Inspect")
                time.sleep(5)
                h2b.browseJsonHistoryAsUrlLastMatch("GLM")
                time.sleep(5)

            # gets the failed to converge, here, after we see it in the browser too
            if emsg is not None:
                raise Exception(emsg)

if __name__ == '__main__':
    h2o.unit_main()
