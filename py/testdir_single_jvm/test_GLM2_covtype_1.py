import unittest, time, sys, random 
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_glm, h2o_import as h2i, h2o_jobs, h2o_exec as h2e

DO_POLL = False
class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.init(java_heap_GB=4)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_GLM2_covtype_1(self):

        csvFilename = 'covtype.data'
        csvPathname = 'standard/' + csvFilename
        hex_key = "covtype.hex"

        parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathname, hex_key=hex_key, schema='local', timeoutSecs=20)

        print "Gratuitous use of frame splitting. result not used"
        fs = h2o.nodes[0].frame_split(source=hex_key, ratios=0.75)
        split0_key = fs['split_keys'][0]
        split1_key = fs['split_keys'][1]
        split0_row = fs['split_rows'][0]
        split1_row = fs['split_rows'][1]
        split0_ratio = fs['split_ratios'][0]
        split1_ratio = fs['split_ratios'][1]

        print "WARNING: max_iter set to 8 for benchmark comparisons"
        max_iter = 8

        y = 54
        modelKey = "GLMModel"
        kwargs = {
            # 'cols': x, # for 2
            'response': 'C' + str(y+1), # for 2
            'family': 'binomial',
            # 'link': 'logit', # 2 doesn't support
            'n_folds': 2,
            'max_iter': max_iter,
            'beta_epsilon': 1e-3,
            'destination_key': modelKey
            }

        # maybe go back to simpler exec here. this was from when Exec failed unless this was used
        execExpr="A.hex=%s" % parseResult['destination_key']
        h2e.exec_expr(execExpr=execExpr, timeoutSecs=30)
        # class 1=1, all else 0
        execExpr="A.hex[,%s]=(A.hex[,%s]>%s)" % (y+1, y+1, 1)
        h2e.exec_expr(execExpr=execExpr, timeoutSecs=30)
        aHack = {'destination_key': 'A.hex'}

        timeoutSecs = 120
        # L2 
        start = time.time()
        kwargs.update({'alpha': 0, 'lambda': 0})

        def completionHack(jobKey, modelKey):
            if DO_POLL: # not needed
                pass
            else: 
                h2o_jobs.pollStatsWhileBusy(timeoutSecs=300, pollTimeoutSecs=300, retryDelaySecs=5)
            # print "FIX! how do we get the GLM result"
            params = {'_modelKey': modelKey}
            a = h2o.nodes[0].completion_redirect(jsonRequest="2/GLMModelView.json", params=params)

            # print "GLM result from completion_redirect:", h2o.dump_json(a)
    
        glmFirstResult = h2o_cmd.runGLM(parseResult=aHack, timeoutSecs=timeoutSecs, noPoll=not DO_POLL, **kwargs)
        completionHack(glmFirstResult['job_key'], modelKey)
        print "glm (L2) end on ", csvPathname, 'took', time.time() - start, 'seconds'
        ## h2o_glm.simpleCheckGLM(self, glm, 13, **kwargs)

        # Elastic
        kwargs.update({'alpha': 0.5, 'lambda': 1e-4})
        start = time.time()
        glmFirstResult = h2o_cmd.runGLM(parseResult=aHack, timeoutSecs=timeoutSecs, noPoll=not DO_POLL, **kwargs)
        completionHack(glmFirstResult['job_key'], modelKey)
        print "glm (Elastic) end on ", csvPathname, 'took', time.time() - start, 'seconds'
        ## h2o_glm.simpleCheckGLM(self, glm, 13, **kwargs)

        # L1
        kwargs.update({'alpha': 1, 'lambda': 1e-4})
        start = time.time()
        glmFirstResult = h2o_cmd.runGLM(parseResult=aHack, timeoutSecs=timeoutSecs, noPoll=not DO_POLL, **kwargs)
        completionHack(glmFirstResult['job_key'], modelKey)
        print "glm (L1) end on ", csvPathname, 'took', time.time() - start, 'seconds'
        ## h2o_glm.simpleCheckGLM(self, glm, 13, **kwargs)

if __name__ == '__main__':
    h2o.unit_main()
