import os, json, unittest, time, shutil, sys, re
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts, h2o_glm
import h2o_browse as h2b

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
        global localhost
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(1)
        else:
            h2o_hosts.build_cloud_with_hosts(1)
        h2b.browseTheCloud()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud(h2o.nodes)
    
    def test_GLM_twovalues(self):
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
            -4"

        twoValueList = [
            ('A','B',0, 14),
            ('A','B',1, 14),
            (0,1,0, 12),
            (0,1,1, 12),
            (0,1,'NaN', 12),
            (1,0,'NaN', 12),
            (-1,1,0, 12),
            (-1,1,1, 12),
            (-1e1,1e1,1e1, 12),
            (-1e1,1e1,-1e1, 12),
            ]

        trial = 0
        for (outputTrue, outputFalse, case, coeffNum) in twoValueList:
            write_syn_dataset(csvPathname, 20, 
                rowDataTrue, rowDataFalse, str(outputTrue), str(outputFalse))

            start = time.time()
            key = csvFilename + "_" + str(trial)
            kwargs = {'case': case, 'y': 10, 'family': 'binomial', 'alpha': 0, 'beta_epsilon': 0.0002}

            # default takes 39 iterations? play with alpha/beta
            glm = h2o_cmd.runGLM(csvPathname=csvPathname, key=key)
            h2o_glm.simpleCheckGLM(self, glm, 0, **kwargs)

            # check that the number of entries in coefficients is right (12 with intercept)
            coeffNum = len(glm['GLMModel']['coefficients'])
            if (coeffNum!=coeffNum):
                raise Exception("Should be " + coeffNum + " coefficients in result. %s" % coeffNum)

            print "trial #", trial, "glm end on ", csvFilename, 'took', time.time() - start, 'seconds'
            h2b.browseJsonHistoryAsUrlLastMatch("GLM")
            h2o.check_sandbox_for_errors()
            trial += 1

if __name__ == '__main__':
    h2o.unit_main()
