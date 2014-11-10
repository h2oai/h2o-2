import unittest, random, sys, time
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_glm, h2o_import as h2i, h2o_exec as h2e

def define_params():
    paramDict = {
        'standardize': [None, 0,1],
        'beta_epsilon': [None, 0.0001],
        'family': ['binomial'],
        'n_folds': [0],
        'max_iter': [9],
        }
    return paramDict

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = h2o.setup_random_seed()
        h2o.init()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_GLM2_binomial_goalies(self):
        csvPathname = 'poisson/Goalies.csv'
        print "\nParsing", csvPathname
        parseResult = h2i.import_parse(bucket='smalldata', path=csvPathname, schema='put', hex_key="A.hex")
        inspect = h2o_cmd.runInspect(None, "A.hex")
        # need more info about the dataset for debug
        h2o_cmd.infoFromInspect(inspect, csvPathname)
        case = 20
        execExpr="A.hex[,%s]=(A.hex[,%s]>%s)" % (6+1, 6+1, case)
        h2e.exec_expr(execExpr=execExpr, timeoutSecs=30)


        paramDict = define_params()
        for trial in range(5):
            # params is mutable. This is default.
            # FIX! does it never end if we don't have alpha specified?
            params = {
                'response': 6, 
                'n_folds': 1, 
                'family': "binomial", 
                'alpha': 0,
                # seems we always need a little regularization
                'lambda': 1e-4,
                'beta_epsilon': 0.001, 
                'max_iter': 8
                }

            colX = h2o_glm.pickRandGlmParams(paramDict, params)
            kwargs = params.copy()

            # make timeout bigger with xvals
            timeoutSecs = 180 + (kwargs['n_folds']*30)
            # or double the 4 seconds per iteration (max_iter+1 worst case?)
            timeoutSecs = max(timeoutSecs, (8 * (kwargs['max_iter']+1)))

            start = time.time()
            print "May not solve. Expanded categorical columns causing a large # cols, small # of rows"
            glm = h2o_cmd.runGLM(timeoutSecs=timeoutSecs, parseResult={'destination_key': 'A.hex'}, **kwargs)
            elapsed = time.time()-start
            print "glm end on ", csvPathname, "Trial #", trial, "completed in", elapsed, "seconds.",\
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)

            h2o_glm.simpleCheckGLM(self, glm, None, **kwargs)
            print "Trial #", trial, "completed\n"


if __name__ == '__main__':
    h2o.unit_main()
