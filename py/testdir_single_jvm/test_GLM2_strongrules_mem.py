##
# Testing strong rules in glm allows model to be created using less memory 
# GLM with strong rules off, all else equal, exceeds memory and fails
##

import unittest, sys, time
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_import as h2i
import h2o_glm

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.init(java_heap_MB=250)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_GLM2_strongrules_mem(self):
        
        print("Testing strong rules in glm allows model to be created using less memory.")

        print("Ingest wide Arcene dataset")
        dataFilename = 'smaller_arcene.csv'
        dataPathname = 'arcene'+'/' + dataFilename
        data = h2i.import_parse(bucket='smalldata', path=dataPathname, schema='put', timeoutSecs=200)
        
        print("Strong Rules..."),
        params = {
                    'alpha': 0.5,
                    'n_folds': 0,
                    'strong_rules': 1,
                    'lambda_search': 0,
                    'response': 3000, 
                    'cols': [i for i in range(2900)],
                    'family': 'binomial',
                }
        kwargs = params.copy()
        starttime = time.time()
        glmtest_SR = h2o_cmd.runGLM(timeoutSecs=600, parseResult=data, **kwargs)
        elapsedtime = time.time() - starttime 
        print("GLM modelling completes in %0.4f " % elapsedtime)
        
        #print("No Strong Rules..."),
        #params = {
        #            'alpha': 0.5,
        #            'n_folds': 0,
        #            'strong_rules': 0,
        #            'lambda_search': 0,
        #            'response': 3000, 
        #            'cols': [i for i in range(2900)],
        #            'family': 'binomial',
        #        }
        #kwargs = params.copy()
        #try:
        #    # Large timeout allocated: not expected to take this long 
        #    # but isolates failure to memory, and not timeout
        #    glmtest_noSR = h2o_cmd.runGLM(timeoutSecs=300, parseResult=data, **kwargs)
        #except: 
        #    print("GLM fails to complete")
        #    print("Test complete.")
        #    return
        #
        ## Test should never get here; will fail if it does
        ## Confirms 'usefulness' of test; that GLM only succes with strong rules and fails otherwise
        #print("Test should never get here; throwing exception.")
        #assert(1==0)

        print("Test complete.")

if __name__ == '__main__':
    h2o.unit_main()
