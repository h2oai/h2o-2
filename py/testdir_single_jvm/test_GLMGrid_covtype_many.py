import unittest, time, sys, random
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_glm, h2o_hosts, h2o_import as h2i, h2o_jobs

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(1,java_heap_GB=28)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_GLMGrid_covtype_many(self):
        csvFilename = 'covtype.data'
        csvPathname = 'standard/' + csvFilename
        parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathname, schema='put', timeoutSecs=10)
        inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
        print "\n" + csvPathname, \
            "    num_rows:", "{:,}".format(inspect['num_rows']), \
            "    num_cols:", "{:,}".format(inspect['num_cols'])

        x = ""

        print "WARNING: max_iter set to 8 for benchmark comparisons"
        max_iter = 8

        y = "54"
        kwargs = {
            'x': x,
            'y': y,
            'family': 'binomial',
            'link': 'logit',
            'n_folds': 2,
            'case_mode': '=',
            'case': 1,
            'max_iter': max_iter,
            'beta_eps': 1e-3,
            'lambda': '0,0.5,0.8',
            'alpha': '0,1e-8,1e-4',
            'parallelism': 1,
        }

        start = time.time()
        jobs = []
        totalGLMGridJobs = 0
        for i in range(3):
            GLMResult = h2o_cmd.runGLMGrid(parseResult=parseResult, timeoutSecs=300, noPoll=True, **kwargs)

            # print "GLMResult:", h2o.dump_json(GLMResult)
            job_key = GLMResult['response']['redirect_request_args']['job']
            model_key = GLMResult['response']['redirect_request_args']['destination_key']
            jobs.append( (job_key, model_key) )
            totalGLMGridJobs += 1

        # do some parse work in parallel. Don't poll for parse completion
        # don't bother checking the parses when they are completed (pollWaitJobs looks at all)
        for i in range(10):
            time.sleep(3)
            hex_key = str(i) + ".hex"
            src_key = str(i) + ".src"
            parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathname, schema='put', 
                src_key=src_key, hex_key=hex_key, 
                timeoutSecs=10, noPoll=True, doSummary=False)

        h2o_jobs.pollWaitJobs(timeoutSecs=300)
        elapsed = time.time() - start

        for job_key, model_key in jobs:
            GLMResult = h2o.nodes[0].GLMGrid_view(job=job_key, destination_key=model_key)
            h2o_glm.simpleCheckGLMGrid(self, GLMResult, **kwargs)

        print "All GLMGrid jobs completed in", elapsed, "seconds."
        print "totalGLMGridJobs:", totalGLMGridJobs


if __name__ == '__main__':
    h2o.unit_main()
