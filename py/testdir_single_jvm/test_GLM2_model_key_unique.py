import unittest, time, sys
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_import as h2i, h2o_exec as h2e

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.init()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_GLM2_model_key_unique(self):
        modelKeyDict = {}
        for trial in range (1,5):
            csvPathname = 'iris/iris2.csv'
            start = time.time()
                        # make sure each parse is unique dest key (not in use
            hex_key = "iris2_" + str(trial) + ".hex"
            parseResult = h2i.import_parse(bucket='smalldata', path=csvPathname, schema='put', 
                hex_key=hex_key, timeoutSecs=10)
            y = 4
            execExpr="%s[,%s]=(%s[,%s]==%s)" % (hex_key, y+1, hex_key, y+1, 1)
            h2e.exec_expr(execExpr=execExpr, timeoutSecs=30)
            
            # h2o.py now sets destination_key for a fixed default model name, 
            # we want h2o to create model names for this test, so use none here
            kwargs = {'destination_key': None, 'response':4, 'family': 'gaussian'}
            glmResult = h2o_cmd.runGLM(parseResult=parseResult, timeoutSecs=10, noPoll=True, **kwargs )
            print "GLM #%d" % trial,  "started on ", csvPathname, 'took', time.time() - start, 'seconds'

            model_key = glmResult['destination_key']
            print "GLM model_key:", model_key
            if model_key in modelKeyDict:
                raise Exception("same model_key used in GLM #%d that matches prior GLM #%d" % (trial, modelKeyDict[model_key]))
            modelKeyDict[model_key] = trial

        # just show the jobs still going, if any. maybe none, because short (iris)
        a = h2o.nodes[0].jobs_admin()
        h2o.verboseprint("jobs_admin():", h2o.dump_json(a))


if __name__ == '__main__':
    h2o.unit_main()

