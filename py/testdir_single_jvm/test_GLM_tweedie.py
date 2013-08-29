import unittest, time, sys
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_glm, h2o_hosts

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

    def test_B_benign(self):
        h2o.nodes[0].log_view()
        namelist = h2o.nodes[0].log_download()

        print "\nStarting tweede.csv"
        csvFilename = "AutoClaim.csv"
        csvPathname = h2o.find_dataset('tweedie/' + csvFilename)
        parseResult = h2o_cmd.parseFile(csvPathname=csvPathname, key2=csvFilename + ".hex")
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
                'weight': 1.0,
                'n_folds': 0,
                'beta_epsilon': 1e-4,
        }

        glm = h2o_cmd.runGLMOnly(parseResult=parseResult, timeoutSecs=15, **kwargs)
        (warnings, coefficients, intercept) = h2o_glm.simpleCheckGLM(self, glm, None, **kwargs)
        coefficients.append( intercept )
        print 'coefficients: %s' % (str(coefficients))
        coefTruth = [ -0.017, -0.009, -0.004, -0.054, 0.013, -0.006, 0.006, -0.017, -0.013, -0.004, 0.144 ]

        deltaCoeff = deltaIntcpt = 0.05
        for i,c in enumerate(coefficients):
            g = coefTruth[ i ]
            print "coefficient[%d]: %8.4f,    truth: %8.4f,    delta: %8.4f" % (i, c, g, abs(g-c))
            self.assertAlmostEqual(c, g, delta=deltaCoeff, msg="not close enough. coefficient[%d]: %s,    generated %s" % (i, c, g))

        sys.stdout.write('.')
        sys.stdout.flush() 


        h2o.nodes[0].log_view()
        namelist = h2o.nodes[0].log_download()

if __name__ == '__main__':
    h2o.unit_main()
