import unittest, time, sys
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_glm, h2o_hosts, h2o_browse as h2b, h2o_jobs, h2o_import as h2i

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global localhost
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(3)
        else:
            h2o_hosts.build_cloud_with_hosts(3)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_GLM_big1_nopoll(self):
        csvPathname = 'hhp_107_01.data.gz'
        print "\n" + csvPathname

        y = "106"
        x = ""
        parseResult = h2i.import_parse(bucket='smalldata', path=csvPathname, schema='put', timeoutSecs=15)

        glmInitial = []
        # dispatch multiple jobs back to back
        start = time.time()
        for jobDispatch in range(10):
            kwargs = {'x': x, 'y': y, 'n_folds': 1}
            # FIX! what model keys do these get?
            glm = h2o_cmd.runGLM(parseResult=parseResult, timeoutSecs=300, noPoll=True, **kwargs)
            glmInitial.append(glm)
            print "glm job dispatch end on ", csvPathname, 'took', time.time() - start, 'seconds'
            print "\njobDispatch #", jobDispatch

            timeoutSecs = 200
        h2o_jobs.pollWaitJobs(pattern='GLM', timeoutSecs=timeoutSecs, retryDelaySecs=10)
        elapsed = time.time() - start
        print "%d pct. of timeout" % ((elapsed/timeoutSecs) * 100)

        # we saved the initial response?
        # if we do another poll they should be done now, and better to get it that 
        # way rather than the inspect (to match what simpleCheckGLM is expected
        for glm in glmInitial:
            print "Checking completed job, with no polling using initial response:", h2o.dump_json(glm)
        
            a = h2o.nodes[0].poll_url(glm, noPoll=True)
            h2o_glm.simpleCheckGLM(self, a, 'C58', **kwargs)

if __name__ == '__main__':
    h2o.unit_main()
