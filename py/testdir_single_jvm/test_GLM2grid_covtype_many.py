import unittest, time, sys, random
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_glm, h2o_import as h2i, h2o_jobs

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.init(1,java_heap_GB=28)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_GLM2grid_covtype_many(self):
        csvFilename = 'covtype.data'
        csvPathname = 'standard/' + csvFilename
        parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathname, schema='put', timeoutSecs=20)
        inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
        print "\n" + csvPathname, \
            "    numRows:", "{:,}".format(inspect['numRows']), \
            "    numCols:", "{:,}".format(inspect['numCols'])

        print "WARNING: max_iter set to 8 for benchmark comparisons"
        max_iter = 8

        y = "54"
        kwargs = {
            'response': y,
            'family': 'gaussian',
            'n_folds': 2,
            'max_iter': max_iter,
            'beta_epsilon': 1e-3,
            'lambda': '0,0.5,0.8',
            'alpha': '0,1e-8,1e-4',
        }

        start = time.time()
        jobs = []
        totalGLMGridJobs = 0
        for i in range(3):
            glmResult = h2o_cmd.runGLM(parseResult=parseResult, timeoutSecs=300, noPoll=True, **kwargs)

            # print "glmResult:", h2o.dump_json(glmResult)
            # assuming it doesn't complete right away, this is the first response
            # it differs for the last response
            job_key = glmResult['job_key']
            grid_key = glmResult['destination_key']
            jobs.append( (job_key, grid_key) )
            totalGLMGridJobs += 1

        # do some parse work in parallel. Don't poll for parse completion
        # don't bother checking the parses when they are completed (pollWaitJobs looks at all)
        for i in range(4):
            time.sleep(3)
            hex_key = str(i) + ".hex"
            src_key = str(i) + ".src"
            parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathname, schema='put', 
                src_key=src_key, hex_key=hex_key, 
                timeoutSecs=10, noPoll=True, doSummary=False)

        h2o_jobs.pollWaitJobs(timeoutSecs=300)
        elapsed = time.time() - start

        # 2/GLMGridView.html?grid_key=asd
        # 2/GLMModelView.html?_modelKey=asd_0&lambda=NaN
        # 2/SaveModel.html?model=GLMGridResults__9a29646b78dd988aacd4f88e4d864ccd_1&path=adfs&force=1
        for job_key, grid_key in jobs:
            gridResult = h2o.nodes[0].glm_grid_view(grid_key=grid_key)
            h2o_glm.simpleCheckGLMGrid(self, gridResult, **kwargs)

        print "All GLMGrid jobs completed in", elapsed, "seconds."
        print "totalGLMGridJobs:", totalGLMGridJobs


if __name__ == '__main__':
    h2o.unit_main()
