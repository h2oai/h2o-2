import unittest, time, sys
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(1)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_glm_model_key_unique(self):
        modelKeyDict = {}
        for trial in range (1,5):
            csvPathname = h2o.find_file('smalldata/iris/iris2.csv')
            start = time.time()
            kwargs = {'y':4, 'family': 'binomial', 'case': 1, 'case_mode': '>'}
            
            # make sure each parse is unique dest key (not in use
            key2 = "iris2_" + str(trial) + ".hex"
            glmResult = h2o_cmd.runGLM(csvPathname=csvPathname, key2=key2, timeoutSecs=10, noPoll=True, **kwargs )
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

