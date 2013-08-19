import unittest, random, sys, time, math
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_hosts, h2o_browse as h2b, h2o_import as h2i, h2o_glm


if 1==0: # works
    DATA_VALUE_MIN = -1
    DATA_VALUE_MAX = 1
    COEFF_VALUE_MIN = -1
    COEFF_VALUE_MAX = 1
    INTCPT_VALUE_MIN = -1
    INTCPT_VALUE_MAX = 1
    # COL_DATA_DISTS = 'UNIQUE'
    COL_DATA_DISTS = 'SAME'

if 1==0: # works
    DATA_VALUE_MIN = -1
    DATA_VALUE_MAX = 0
    COEFF_VALUE_MIN = -1
    COEFF_VALUE_MAX = 1
    INTCPT_VALUE_MIN = -1
    INTCPT_VALUE_MAX = 1
    # COL_DATA_DISTS = 'UNIQUE'
    COL_DATA_DISTS = 'SAME'

if 1==1:
    DATA_VALUE_MIN = -1
    DATA_VALUE_MAX = 1
    COEFF_VALUE_MIN = 0.1
    COEFF_VALUE_MAX = 1
    INTCPT_VALUE_MIN = -1
    INTCPT_VALUE_MAX = 1
    # COL_DATA_DISTS = 'UNIQUE'
    COL_DATA_DISTS = 'SAME'


modeString = \
    "_Dmin" + str(DATA_VALUE_MIN) + \
    "_Dmax" + str(DATA_VALUE_MAX) + \
    "_Cmin" + str(COEFF_VALUE_MIN) + \
    "_Cmax" + str(COEFF_VALUE_MAX) + \
    "_Imin" + str(INTCPT_VALUE_MIN) + \
    "_Imax" + str(INTCPT_VALUE_MAX) + \
    "_Ddist" + str(COL_DATA_DISTS)

print "modeString:", modeString

def gen_rand_equation(colCount, SEED):
    r1 = random.Random(SEED)
    coefficients = []
    # y = 1/(1 + exp(-(sum(coefficients*x)+intercept))
    for j in range(colCount):
        # ri = r1.randint(-1,1)
        rif = r1.uniform(COEFF_VALUE_MIN, COEFF_VALUE_MAX)
        # rif = (j+0.0)/colCount # git bigger for each one
        coefficients.append(rif)
        # FIX! temporary try fixed = col+1
        # coefficients.append(j+1)
        # coefficients.append(2 + 2*(j%2))

    intercept = r1.uniform(INTCPT_VALUE_MIN, INTCPT_VALUE_MAX)
    # intercept =  0
    print "Expected coefficients:", coefficients
    print "Expected intercept:", intercept

    return(coefficients, intercept) 

# FIX! random noise on coefficients? randomly force 5% to 0?  
#y = 1/(1 + math.exp(-(sum(coefficients*x)+intercept)) 
def yFromEqnAndData(coefficients, intercept, rowData):
    # FIX! think about using noise on some of the rowData
    cx = [a*b for a,b in zip(coefficients, rowData)]
    y = 1/(1 + math.exp(-(sum(cx) + intercept)))
    if (y<0 or y>1):
        raise Exception("Generated y result is should be between 0 and 1: " + y)
    return y

def write_syn_dataset(csvPathname, rowCount, colCount, coefficients, intercept, SEED, noise=0.05):
    r1 = random.Random(SEED)
    dsf = open(csvPathname, "w+")

    # assuming output is always last col
    yMin = None  
    yMax = None
    BINS = 100
    print "gen'ed y will be a probability! generate 1/0 data rows to reflect that probability, binned to %d bins" % BINS
    print "100 implies 2 places of accuracy in getting the probability." 
    print  "this means we should get 1 place of accuracy in the result coefficients/intercept????"
    # generate a mode per column that is reused
    # this will make every column have a different data distribution
    if COL_DATA_DISTS == 'UNIQUE':
        colModes = [((random.randint(0,1) * -1) * j/colCount) for j in range(colCount)]
    elif COL_DATA_DISTS == 'SAME':
        colDataMean = (DATA_VALUE_MAX - DATA_VALUE_MIN) / 2
        colModes = [colDataMean for j in range(colCount)]
    else: # random
        colModes = [r1.uniform(DATA_VALUE_MIN, DATA_VALUE_MAX) for j in range(colCount)]

    for i in range(rowCount):
        rowData = []
        for j in range(colCount):
            # ri = r1.uniform(0,1) 
            ri = r1.triangular(DATA_VALUE_MIN, DATA_VALUE_MAX, colModes[j])
            rowData.append(ri)
        
        # flip if within the noise percentage
        # FIX! not used right now
        if (noise is not None) and (r1.random() <= noise): 
            flip = True
        else: 
            flip = False
        
        # Do a walk from 0 to 1 by .1
        # writing 0 or 1 depending on whether you are below or above the probability
        # coarse approximation to get better coefficient match in GLM
        y = yFromEqnAndData(coefficients, intercept, rowData)

        if yMin is None or y<yMin: yMin = y
        if yMax is None or y>yMax: yMax = y
        
        for i in range(1,BINS+1): # 10 bins
            if y > (i + 0.0)/BINS:
                binomial = 1
            else:
                binomial = 0
            rowDataCsv = ",".join(map(str,rowData + [binomial]))
            dsf.write(rowDataCsv + "\n")

    dsf.close()
    print "yMin:", yMin, " yMax:", yMax


class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED, localhost
        SEED = h2o.setup_random_seed()
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(1,java_heap_GB=12)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        ### time.sleep(3600)
        h2o.tear_down_cloud()

    def test_GLM_with_logit_depcols(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        tryList = [
            # (100, 1, 'cA', 300), 
            # (100, 25, 'cB', 300), 
            # (1000, 25, 'cC', 300), 
            # 50 fails, 40 fails
            # (10000, 50, 'cD', 300), 
            # 30 passes
            # (10000, 30, 'cD', 300), 
            # 50 passed if I made the data distributions per col, unique by guaranteeing different triangular modes per col
            (1000, 30, 'cD', 300), 
            (1000, 30, 'cD', 300), 
            ]

        ### h2b.browseTheCloud()
        lenNodes = len(h2o.nodes)

        for (rowCount, colCount, key2, timeoutSecs) in tryList:
            SEEDPERFILE = random.randint(0, sys.maxint)
            csvFilename = 'syn_' + modeString + "_" + str(SEEDPERFILE) + "_" + str(rowCount) + 'x' + str(colCount) + '.csv'
            csvPathname = SYNDATASETS_DIR + '/' + csvFilename

            print "Creating random", csvPathname, \
                "using random coefficients and intercept and logit eqn. for output"
            (coefficientsGen, interceptGen) = gen_rand_equation(colCount, SEEDPERFILE)
            print coefficientsGen, interceptGen
            write_syn_dataset(csvPathname, rowCount, colCount, coefficientsGen, interceptGen, SEEDPERFILE)

            parseKey = h2o_cmd.parseFile(None, csvPathname, key2=key2, timeoutSecs=60)
            print csvFilename, 'parse time:', parseKey['response']['time']
            print "Parse result['destination_key']:", parseKey['destination_key']

            # We should be able to see the parse result?
            inspect = h2o_cmd.runInspect(None, parseKey['destination_key'])
            print "\n" + csvFilename

            y = colCount
            print "GLM is ignoring the thresholds I give it? deciding what's best?"
            kwargs = {
                    'y': y, 
                    'max_iter': 60, 
                    'lambda': 0.0,
                    'alpha': 0.0,
                    'weight': 1.0,
                    'n_folds': 0,
                    'beta_epsilon': 1e-4,
                    # 'thresholds': 0.5,
                    }

            start = time.time()
            glm = h2o_cmd.runGLMOnly(parseKey=parseKey, timeoutSecs=timeoutSecs, **kwargs)
            (warnings, coefficients, intercept) = h2o_glm.simpleCheckGLM(self, glm, 0, **kwargs)
            print "glm end on ", csvPathname, 'took', time.time() - start, 'seconds'

            for i,c in enumerate(coefficients):
                g = coefficientsGen[i] # generated
                print "coefficient[%d]: %8.4f generated: %8.4f delta: %8.4f" % (i, c, g, abs(g-c))
                self.assertAlmostEqual(c, g, delta=.1, msg="not close enough. coefficient[%d]: %s generated %s" % (i, c, g))

            c = intercept
            g = interceptGen
            print "intercept: %8.4f generated: %8.4f delta: %8.4f" % (c, g, abs(g-c))
            print "Why do we need larger delta allowed for intercept? 0.2 here"
            self.assertAlmostEqual(c, g, delta=.2, msg="not close enough. intercept: %s generated %s" % (c, g))
            

if __name__ == '__main__':
    h2o.unit_main()
