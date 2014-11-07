import unittest, random, sys, time, math
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i, h2o_glm

BINS = 100
def gen_rand_equation(colCount,
    INTCPT_VALUE_MIN, INTCPT_VALUE_MAX,
    COEFF_VALUE_MIN, COEFF_VALUE_MAX, SEED):
    r1 = random.Random(SEED)
    coefficients = []
    # y = 1/(1 + exp(-(sum(coefficients*x)+intercept))
    for j in range(colCount):
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
def yFromEqnAndData(coefficients, intercept, rowData, DATA_DISTS, ALGO):
    # FIX! think about using noise on some of the rowData
    cx = [a*b for a,b in zip(coefficients, rowData)]
    if ALGO=='binomial':
        y = 1.0/(1.0 + math.exp(-(sum(cx) + intercept)))
        if y<0 or y>1:
            raise Exception("Generated y result is should be between 0 and 1: %s" % y)
    elif ALGO=='poisson':
        y = math.exp(sum(cx) + intercept)
        if y<0:
            raise Exception("Generated y result is should be >= 0: %s" % y)
    elif ALGO=='gamma':
        y = 1.0/(sum(cx) + intercept)
        if y<0:
            raise Exception("Generated y result is should be >= 0: %s" % y)
    else:
        raise Exception('Unknown ALGO: %s' % ALGO)

    return y

def write_syn_dataset(csvPathname, rowCount, colCount, coefficients, intercept, 
    DATA_VALUE_MIN, DATA_VALUE_MAX, DATA_DISTS, ALGO, SEED):
    r1 = random.Random(SEED)
    dsf = open(csvPathname, "w+")

    # assuming output is always last col
    yMin = None  
    yMax = None
    # generate a mode per column that is reused
    # this will make every column have a different data distribution
    if DATA_DISTS == 'unique_pos_neg':
        d = DATA_VALUE_MIN
        fullRange= DATA_VALUE_MAX - DATA_VALUE_MIN
        colModes = []
        for j in range(colCount):
            colModes += [(random.randint(0,1) * -1) * (((float(j)/colCount) * fullRange) + DATA_VALUE_MIN)]

    elif DATA_DISTS == 'mean':
        colDataMean = (DATA_VALUE_MIN + DATA_VALUE_MAX) / 2
        colModes = [colDataMean for j in range(colCount)]

    elif DATA_DISTS == 'random':
        colModes = [r1.uniform(DATA_VALUE_MIN, DATA_VALUE_MAX) for j in range(colCount)]

    else: 
        raise Exception('Unknown DATA_DIST: %s' % DATA_DIST)

    print "\ncolModes:", colModes
    if ALGO=='binomial':
        print "gen'ed y is probability! generate 1/0 data rows wth that probability, binned to %d bins" % BINS
        print "100 implies 2 places of accuracy in getting the probability." 
        print  "this means we should get 1 place of accuracy in the result coefficients/intercept????"
    for i in range(rowCount):
        rowData = []
        for j in range(colCount):
            # ri = r1.uniform(0,1) 
            ri = r1.triangular(DATA_VALUE_MIN, DATA_VALUE_MAX, colModes[j])
            rowData.append(ri)
        
        # Do a walk from 0 to 1 by .1
        # writing 0 or 1 depending on whether you are below or above the probability
        # coarse approximation to get better coefficient match in GLM
        y = yFromEqnAndData(coefficients, intercept, rowData, DATA_DISTS, ALGO)
        if yMin is None or y<yMin: yMin = y
        if yMax is None or y>yMax: yMax = y

        if ALGO=='binomial':
            for i in range(1,BINS+1): # 10 bins
                if y > (i + 0.0)/BINS:
                    binomial = 1
                else:
                    binomial = 0
                rowDataCsv = ",".join(map(str,rowData + [binomial]))
                dsf.write(rowDataCsv + "\n")

        elif ALGO=='poisson':
            rowDataCsv = ",".join(map(str,rowData + [int(y)]))
            dsf.write(rowDataCsv + "\n")

        elif ALGO=='gamma':
            rowDataCsv = ",".join(map(str,rowData + [y]))
            dsf.write(rowDataCsv + "\n")

        else:
            raise Exception('Unknown ALGO: %s' % ALGO)

    dsf.close()
    print "yMin:", yMin, " yMax:", yMax


class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = h2o.setup_random_seed()
        h2o.init(1,java_heap_GB=12)

    @classmethod
    def tearDownClass(cls):
        ### time.sleep(3600)
        h2o.tear_down_cloud()

    #************************************************************************************
    def GLM_syn_eqns_data(self,
        ALGO='binomial', 
        DATA_VALUE_MIN=-1, DATA_VALUE_MAX=1,
        COEFF_VALUE_MIN=-1, COEFF_VALUE_MAX=1, 
        INTCPT_VALUE_MIN=-1, INTCPT_VALUE_MAX=1,
        DATA_DISTS='unique_pos_neg'):

        SYNDATASETS_DIR = h2o.make_syn_dir()

        if ALGO=='poisson':
            tryList = [
                (50000, 5, 'cD', 300), 
                ]
        else:
            tryList = [
                # (100, 1, 'cA', 300), 
                # (100, 25, 'cB', 300), 
                # (1000, 25, 'cC', 300), 
                # 50 fails, 40 fails
                # (10000, 50, 'cD', 300), 
                # 30 passes
                # (10000, 30, 'cD', 300), 
                # 200 passed
                (500, 30, 'cD', 300), 
                (5000, 30, 'cD', 300), 
                ]

        ### h2b.browseTheCloud()
        lenNodes = len(h2o.nodes)

        for (rowCount, colCount, hex_key, timeoutSecs) in tryList:
            modeString = \
                "_Bins" + str(BINS) + \
                "_Dmin" + str(DATA_VALUE_MIN) + \
                "_Dmax" + str(DATA_VALUE_MAX) + \
                "_Cmin" + str(COEFF_VALUE_MIN) + \
                "_Cmax" + str(COEFF_VALUE_MAX) + \
                "_Imin" + str(INTCPT_VALUE_MIN) + \
                "_Imax" + str(INTCPT_VALUE_MAX) + \
                "_Ddist" + str(DATA_DISTS)
            print "modeString:", modeString

            SEEDPERFILE = random.randint(0, sys.maxint)
            csvFilename = 'syn_' + modeString + "_" + str(SEEDPERFILE) + "_" + str(rowCount) + 'x' + str(colCount) + '.csv'
            csvPathname = SYNDATASETS_DIR + '/' + csvFilename

            print "Creating random", csvPathname, \
                "using random coefficients and intercept and logit eqn. for output"
            (coefficientsGen, interceptGen) = gen_rand_equation(colCount,
                INTCPT_VALUE_MIN, INTCPT_VALUE_MAX,
                COEFF_VALUE_MIN, COEFF_VALUE_MAX, SEEDPERFILE)
            print coefficientsGen, interceptGen

            write_syn_dataset(csvPathname, rowCount, colCount, coefficientsGen, interceptGen, 
                DATA_VALUE_MIN, DATA_VALUE_MAX, DATA_DISTS, ALGO, SEED)

            parseResult = h2i.import_parse(path=csvPathname, hex_key=hex_key, schema='put', timeoutSecs=60)
            print "Parse result['destination_key']:", parseResult['destination_key']

            # We should be able to see the parse result?
            inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
            print "\n" + csvFilename

            y = colCount
            print "GLM is ignoring the thresholds I give it? deciding what's best?"
            kwargs = {
                    'standardize': 0,
                    # link is default
                    # 'link': 
                    'family': ALGO,
                    'response': y, 
                    'max_iter': 25, 
                    'lambda': 0,
                    'alpha': 0,
                    'n_folds': 0,
                    'beta_epsilon': 1e-4,
                    # 'thresholds': 0.5,
                    }

            start = time.time()
            glm = h2o_cmd.runGLM(parseResult=parseResult, timeoutSecs=timeoutSecs, **kwargs)
            (warnings, coefficients, intercept) = h2o_glm.simpleCheckGLM(self, glm, 'C1', **kwargs)
            print "glm end on ", csvPathname, 'took', time.time() - start, 'seconds'

            if ALGO=='binomial':
                deltaCoeff = 0.1
                deltaIntcpt = 0.2
            else: # poisson needs more? 
                deltaCoeff = 0.5
                deltaIntcpt = 1.0

            for i,c in enumerate(coefficients):
                g = coefficientsGen[i] # generated
                print "coefficient[%d]: %8.4f,    generated: %8.4f,    delta: %8.4f" % (i, c, g, abs(g-c))
                self.assertAlmostEqual(c, g, delta=deltaCoeff, msg="not close enough. coefficient[%d]: %s,    generated %s" % (i, c, g))

            c = intercept
            g = interceptGen
            print "intercept: %8.4f,    generated: %8.4f,    delta: %8.4f" % (c, g, abs(g-c))
            print "need a larger delta compare for intercept?"
            self.assertAlmostEqual(c, g, delta=deltaIntcpt, msg="not close enough. intercept: %s,    generated %s" % (c, g))

    #************************************************************************************
            
    def test_GLM2_syn_eqns_data_A(self):
        self.GLM_syn_eqns_data(
            ALGO='binomial', 
            DATA_VALUE_MIN=-1, DATA_VALUE_MAX=1,
            COEFF_VALUE_MIN=-1, COEFF_VALUE_MAX=1, 
            INTCPT_VALUE_MIN=-1, INTCPT_VALUE_MAX=1,
            DATA_DISTS='unique_pos_neg')

    def test_GLM2_syn_eqns_data_B(self):
        self.GLM_syn_eqns_data(
            ALGO='binomial', 
            DATA_VALUE_MIN=-1, DATA_VALUE_MAX=1,
            COEFF_VALUE_MIN=-1, COEFF_VALUE_MAX=1, 
            INTCPT_VALUE_MIN=-1, INTCPT_VALUE_MAX=1,
            DATA_DISTS='mean')

    def test_GLM2_syn_eqns_data_C(self):
        self.GLM_syn_eqns_data(
            ALGO='poisson', 
            DATA_VALUE_MIN=0, DATA_VALUE_MAX=1,
            COEFF_VALUE_MIN=0, COEFF_VALUE_MAX=1, 
            INTCPT_VALUE_MIN=0, INTCPT_VALUE_MAX=1,
            DATA_DISTS='mean')

    def test_GLM2_syn_eqns_data_D(self):
        # data and y have to be 0 to N for poisson
        self.GLM_syn_eqns_data(
            ALGO='poisson', 
            DATA_VALUE_MIN=0, DATA_VALUE_MAX=1,
            COEFF_VALUE_MIN=0, COEFF_VALUE_MAX=1, 
            INTCPT_VALUE_MIN=0, INTCPT_VALUE_MAX=1,
            DATA_DISTS='unique_pos_neg')

    def test_GLM2_syn_eqns_data_E(self):
        # data and y have to be 0 to N for poisson
        # y seems to be tightly clamped between 0 and 1 if you have coefficient range from -1 to 0
        self.GLM_syn_eqns_data(
            ALGO='poisson', 
            DATA_VALUE_MIN=0, DATA_VALUE_MAX=2,
            COEFF_VALUE_MIN=-.2, COEFF_VALUE_MAX=2, 
            INTCPT_VALUE_MIN=-.2, INTCPT_VALUE_MAX=2,
            DATA_DISTS='random')

    def test_GLM2_syn_eqns_data_F(self):
        # data and y have to be 0 to N for poisson
        # y seems to be tightly clamped between 0 and 1 if you have coefficient range from -1 to 0
        self.GLM_syn_eqns_data(
            ALGO='gamma', 
            DATA_VALUE_MIN=0, DATA_VALUE_MAX=2,
            COEFF_VALUE_MIN=-.2, COEFF_VALUE_MAX=2, 
            INTCPT_VALUE_MIN=-.2, INTCPT_VALUE_MAX=2,
            DATA_DISTS='random')

if __name__ == '__main__':
    h2o.unit_main()
