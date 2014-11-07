import unittest, time, sys, random, copy
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd
import h2o_glm
import h2o_browse as h2b
import h2o_import as h2i
import h2o_exec as h2e

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.init(3,java_heap_GB=4)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_GLM2_from_import_hosts(self):
        csvFilenameList = [
            'covtype.data',
            'covtype20x.data',
            ]

        # a browser window too, just because we can
        ## h2b.browseTheCloud()
        importFolderPath = "standard"
        validation1= {}
        for csvFilename in csvFilenameList:
            # have to re-import each iteration now, since the source key
            # is removed and if we re-parse it, it's not there
            csvPathname = importFolderPath + "/" + csvFilename
            parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathname, hex_key='A.hex', timeoutSecs=2000)
            print "Parse result['destination_key']:", parseResult['destination_key']

            # We should be able to see the parse result?
            inspect = h2o_cmd.runInspect(key=parseResult['destination_key'])
            print "\n" + csvFilename

            start = time.time()
            # can't pass lamba as kwarg because it's a python reserved word
            case = 1
            y = 54
            execExpr="A.hex[,%s]=(A.hex[,%s]==%s)" % (y+1, y+1, case)
            h2e.exec_expr(execExpr=execExpr, timeoutSecs=30)

            kwargs = {'response': y, 'n_folds': 2, 'family': "binomial"}
            glm = h2o_cmd.runGLM(parseResult={'destination_key': 'A.hex'}, timeoutSecs=2000, **kwargs)
            h2o_glm.simpleCheckGLM(self, glm, None, **kwargs)

            # compare this glm to the first one. since the files are replications, the results
            # should be similar?
            validation = glm['glm_model']['submodels'][0]['validation']

            if validation1:
                h2o_glm.compareToFirstGlm(self, 'auc', validation, validation1)
            else:
                validation1 = copy.deepcopy(validation)

if __name__ == '__main__':
    h2o.unit_main()
