import unittest, time, sys, re
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_glm, h2o_browse as h2b, h2o_import as h2i, h2o_exec as h2e

def write_syn_dataset(csvPathname, rowCount, rowDataTrue, rowDataFalse, outputTrue, outputFalse):
    dsf = open(csvPathname, "w+")
    for i in range(int(rowCount/2)):
        dsf.write(rowDataTrue + ',' + outputTrue + "\n")

    for i in range(int(rowCount/2)):
        dsf.write(rowDataFalse + ',' + outputFalse + "\n")
    dsf.close()

class GLM_twovalues(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        # fails with 3
        h2o.init(1)
        # h2b.browseTheCloud()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud(h2o.nodes)
    
    def test_GLM2_twovalues(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        csvFilename = "syn_twovalues.csv"
        csvPathname = SYNDATASETS_DIR + '/' + csvFilename

        # H2O might not do whitespace stripping on numbers correctly, when , is {SEP}
        # GLM will auto expand categoricals..so if we have more coefficients than expected
        # that means it didn't parse right
        # mix in space/tab combos
        # just done like this for readability
        rowDataTrueRaw = \
            "<sp>1,\
            0<sp>,\
            <tab>65,\
            1<tab>,\
            <sp><tab>2,\
            1<sp><tab>,\
            <tab><sp>1,\
            4<tab><sp>,\
            <tab><tab>1,\
            4<tab><tab>,\
            <sp><sp>1,\
            4<sp><sp>"

        rowDataTrue = re.sub("<sp>"," ", rowDataTrueRaw)
        rowDataTrue = re.sub("<tab>","  ", rowDataTrue)

        rowDataFalse = \
            "0,\
            1,\
            0,\
            -1,\
            -2,\
            -1,\
            -1,\
            -4,\
            -1,\
            -4,\
            -1,\
            -3"

        twoValueList = [
            # (0,1,0, 12),
            # (0,1,1, 12),
            # ('A','B',0, 12),
            # ('A','B',1, 12),
            (-1,1,-1, 12),
            (-1,1,1, 12),
            (-1e1,1e1,1e1, 12),
            (-1e1,1e1,-1e1, 12),
            ]

        trial = 0
        for (outputTrue, outputFalse, case, expectedCoeffNum) in twoValueList:
            write_syn_dataset(csvPathname, 20, 
                rowDataTrue, rowDataFalse, str(outputTrue), str(outputFalse))

            hex_key = csvFilename + "_" + str(trial)
            parseResult = h2i.import_parse(path=csvPathname, schema='put', hex_key=hex_key)

            # maybe go back to simpler exec here. this was from when Exec failed unless this was used
            execExpr="A.hex=%s" % hex_key
            h2e.exec_expr(execExpr=execExpr, timeoutSecs=30)
            execExpr="A.hex[,%s]=(A.hex[,%s]==%s)" % (13, 13, case)
            h2e.exec_expr(execExpr=execExpr, timeoutSecs=30)
            aHack = {'destination_key': 'A.hex'}

            start = time.time()
            kwargs = {
                'n_folds': 0,
                'response': 'C13', 
                'family': 'binomial', 
                'alpha': 0.0, 
                'lambda': 0, 
                'beta_epsilon': 0.0002
            }

            # default takes 39 iterations? play with alpha/beta
            print "using outputTrue: %s outputFalse: %s" % (outputTrue, outputFalse)
            glm = h2o_cmd.runGLM(parseResult=aHack, **kwargs)
            (warnings, coefficients, intercept) = h2o_glm.simpleCheckGLM(self, glm, None, **kwargs)

            # check that the number of entries in coefficients is right (12 with intercept)

            coefficients_names = glm['glm_model']['coefficients_names']
            print "coefficients_names:", coefficients_names

            # subtract one for intercept
            actualCoeffNum = len(glm['glm_model']['submodels'][0]['beta']) - 1
            if (actualCoeffNum!=expectedCoeffNum):
                raise Exception("Should be %s expected coefficients in result. actual: %s" % (expectedCoeffNum, actualCoeffNum))

            print "trial #", trial, "glm end on ", csvFilename, 'took', time.time() - start, 'seconds'
            h2o.check_sandbox_for_errors()
            trial += 1

if __name__ == '__main__':
    h2o.unit_main()
