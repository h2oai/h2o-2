import unittest, time, sys
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_glm, h2o_hosts, h2o_import as h2i, h2o_util

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global localhost
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(1)
        else:
            h2o_hosts.build_cloud_with_hosts(1)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_GLM_tweedie(self):
        csvFilename = "AutoClaim.csv"
        csvPathname = 'standard/' + csvFilename
        print "\nStarting", csvPathname
        parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathname, schema='put')
        # columns start at 0
        # regress: glm(CLM_AMT ~ CAR_USE + REVOLKED + GENDER + AREA + MARRIED + CAR_TYPE, data=AutoClaim, family=tweedie(1.34))
        # 
        y = "4"
        coefs = [7, 13, 20, 27, 21, 11]
        # sapply(c('CLM_AMT', 'CAR_USE', 'REVOLKED', 'GENDER', 'AREA', 'MARRIED', 'CAR_TYPE'), function(x) which(x==colnames(AutoClaim)) - 1)
        x = ','.join([ str(x) for x in coefs ])
        kwargs = {
                'family': 'tweedie',
                'tweedie_power': 1.36,
                'y': y, 
                'x' : x,
                'max_iter': 10, 
                'lambda': 0,
                'alpha': 0,
                'n_folds': 0,
                'beta_epsilon': 1e-4,
        }

        glm = h2o_cmd.runGLM(parseResult=parseResult, timeoutSecs=15, **kwargs)
        (warnings, coefficients, intercept) = h2o_glm.simpleCheckGLM(self, glm, None, **kwargs)
        coefficients.append( intercept )
        print 'coefficients: %s' % (str(coefficients))
        coefTruth = [ -0.017, -0.009, -0.004, -0.054, 0.013, -0.006, 0.006, -0.017, -0.013, -0.004, 0.144 ]

        deltaCoeff = deltaIntcpt = 0.05
        for i,c in enumerate(coefficients):
            g = coefTruth[ i ]
            print "coefficient[%d]: %8.4f,    truth: %8.4f,    delta: %8.4f" % (i, c, g, abs(g-c))
            self.assertAlmostEqual(c, g, delta=deltaCoeff, msg="not close enough. coefficient[%d]: %s,    generated %s" % (i, c, g))

if __name__ == '__main__':
    h2o.unit_main()
