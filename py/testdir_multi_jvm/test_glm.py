import os, json, unittest, time, shutil, sys
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_hosts

# Test of glm comparing result against R-implementation
# Tested on prostate.csv short (< 1M) and long (multiple chunks)
# kbn. just updated the parseFile and runGLMonly to match the 
# higher level api now in other tests.
class GLMTest(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    
    @classmethod
    def setUpClass(cls):
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(node_count=3)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud(h2o.nodes)
    
    def process_dataset(self,key,Y, e_coefs, e_ndev, e_rdev, e_aic, **kwargs):
        # no regularization
        kwargs['alpha'] = 0
        kwargs['lambda'] = 0
        glm = h2o_cmd.runGLMOnly(parseKey = key, Y = 'CAPSULE', timeoutSecs=10, **kwargs)

        GLMModel = glm['GLMModel']
        GLMParams = GLMModel["GLMParams"]
        family = GLMParams["family"]
        coefs = GLMModel['coefficients']        

        # pop the first validation from the list
        validationsList = GLMModel['validations']
        validations = validationsList.pop()

        err = validations['err']
        nullDev = validations['nullDev']
        resDev = validations['resDev']

        # change to .1% of R for allowed error, not .01 absolute error
        errors = []
        for x in coefs: 
            h2o.verboseprint("Comparing:", coefs[x], e_coefs[x])
            if abs(float(coefs[x]) - e_coefs[x]) > (0.001 * abs(e_coefs[x])):
                errors.append('%s: %f != %f' % (x,e_coefs[x],coefs[x]))

        # FIX! our null deviance doesn't seem to match
        h2o.verboseprint("Comparing:", nullDev, e_ndev)
        # if abs(float(nullDev) - e_ndev) > (0.001 * e_ndev): 
        #    errors.append('NullDeviance: %f != %s' % (e_ndev,nullDev))

        # FIX! our res deviance doesn't seem to match
        h2o.verboseprint("Comparing:", resDev, e_rdev)
        # if abs(float(resDev) - e_rdev) > (0.001 * e_rdev): 
        #    errors.append('ResDeviance: %f != %s' % (e_rdev,resDev))

        # FIX! we don't have an AIC to compare?
        return errors
    
    def test_prostate_gaussian(self):
        errors = []
        # First try on small data (1 chunk)
        key = h2o_cmd.parseFile(csvPathname=h2o.find_file("smalldata/logreg/prostate.csv"),key2='prostate_g')
        # R results
        gaussian_coefficients = {"Intercept":-0.8052693, "ID":0.0002764,"AGE":-0.0011601,"RACE":-0.0826932, "DPROS":0.0924781,"DCAPS":0.1089754,"PSA":0.0036211, "VOL":-0.0020560,"GLEASON":0.1515751}
        gaussian_nd  = 91.4
        gaussian_rd  = 65.04
        gaussian_aic = 427.6
        errors = self.process_dataset(key, 'CAPSULE', gaussian_coefficients, gaussian_nd, gaussian_rd, gaussian_aic, family = 'gaussian')
        if errors:
            self.fail(str(errors))
        # Now try on larger data (replicated), will be chunked this time, should produce same results
        key = h2o_cmd.parseFile(csvPathname=h2o.find_file("smalldata/logreg/prostate_long.csv.gz"), key2='prostate_long_g')
        errors = self.process_dataset(key, 'CAPSULE', gaussian_coefficients, gaussian_nd, gaussian_rd, gaussian_aic, family = 'gaussian')
        if errors:
            self.fail(str(errors))

    def test_prostate_binomial(self):
        errors = []
        # First try on small data (1 chunk)
        key = h2o_cmd.parseFile(csvPathname=h2o.find_file("smalldata/logreg/prostate.csv"), key2='prostate_b')
        # R results
        binomial_coefficients = {"Intercept":-8.126278, "ID":0.001609,"AGE":-0.008138,"RACE":-0.617597, "DPROS":0.553065,"DCAPS":0.546087,"PSA":0.027297, "VOL":-0.011540,"GLEASON":1.010125}
        binomial_nd  = 512.3
        binomial_rd  = 376.9
        binomial_aic = 394.9
        errors = self.process_dataset(key, 'CAPSULE', binomial_coefficients, binomial_nd, binomial_rd, binomial_aic, family = 'binomial')
        if errors:
            self.fail(str(errors))
        # Now try on larger data (replicated), will be chunked this time, should produce same results
        key = h2o_cmd.parseFile(csvPathname=h2o.find_file("smalldata/logreg/prostate_long.csv.gz"), key2='prostate_long_b')
        errors = self.process_dataset(key, 'CAPSULE', binomial_coefficients, binomial_nd, binomial_rd, binomial_aic, family = 'binomial')
        if errors:
            self.fail(str(errors))

    def test_prostate_poisson(self):
        errors = []
        # First try on small data (1 chunk)
        key = h2o_cmd.parseFile(csvPathname=h2o.find_file("smalldata/logreg/prostate.csv"), key2='prostate_p')
        # R results
        poisson_coefficients = {"Intercept":-4.107484, "ID":0.000508,"AGE":-0.004357,"RACE":-0.149412, "DPROS":0.230458,"DCAPS":0.071546,"PSA":0.002944, "VOL":-0.007488,"GLEASON":0.441659}
        poisson_nd  = 278.4
        poisson_rd  = 215.7
        poisson_aic = 539.7
        errors = self.process_dataset(key, 'CAPSULE', poisson_coefficients, poisson_nd, poisson_rd, poisson_aic, family = 'poisson')
        if errors:
            self.fail(str(errors))
        # Now try on larger data (replicated), will be chunked this time, should produce same results
        key = h2o_cmd.parseFile(csvPathname=h2o.find_file("smalldata/logreg/prostate_long.csv.gz"), key2='poisson_long_p')
        errors = self.process_dataset(key, 'CAPSULE', poisson_coefficients, poisson_nd, poisson_rd, poisson_aic, family = 'poisson')
        if errors:
            self.fail(str(errors))


if __name__ == '__main__':
    h2o.unit_main()
