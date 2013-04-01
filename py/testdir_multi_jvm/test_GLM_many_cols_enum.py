import unittest
import random, sys, time, os
sys.path.extend(['.','..','py'])

# FIX! add cases with shuffled data!
import h2o, h2o_cmd, h2o_hosts, h2o_glm
import h2o_browse as h2b, h2o_import as h2i, h2o_exec as h2e

def write_syn_dataset(csvPathname, rowCount, colCount, SEED, translateList):
    # do we need more than one random generator?
    r1 = random.Random(SEED)
    r2 = random.Random(SEED)
    dsf = open(csvPathname, "w+")

    for i in range(rowCount):
        rowData = []
        for j in range(colCount):
            ### ri1 = int(r1.triangular(0,2,1.5))
            ri1 = int(r1.triangular(0,4,2.5))
            rowData.append(ri1)

        rowTotal = sum(rowData)
        if translateList is not None:
            for i, iNum in enumerate(rowData):
                rowData[i] = translateList[iNum]

        result = r2.randint(0,1)
        ### print colCount, rowTotal, result
        rowDataStr = map(str,rowData)
        rowDataStr.append(str(result))
        # add the output twice, to try to match to it?
        # can't include output in input, pefect separation, failure to converge?
        ### rowDataStr.append(str(result))

        rowDataCsv = ",".join(rowDataStr)
        dsf.write(rowDataCsv + "\n")

    dsf.close()

paramDict = {
    'family': ['binomial'],
    'lambda': [1.0E-5],
    'alpha': [1.0],
    'max_iter': [50],
    'weight': [1.0],
    'thresholds': [0.5],
    'num_cross_validation_folds': [2],
    'beta_epsilon': [1.0E-4],
    }

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = random.randint(0, sys.maxint)
        # SEED = 
        random.seed(SEED)
        print "\nUsing random seed:", SEED
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(3,java_heap_GB=3,use_flatfile=True)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_GLM_many_cols_enum(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        translateList = ['a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u']
        tryList = [
            (10000,  100, 'cA', 100),
            (10000,  200, 'cB', 200),
            (10000,  300, 'cC', 300),
            ]

        ### h2b.browseTheCloud()

        for (rowCount, colCount, key2, timeoutSecs) in tryList:
            SEEDPERFILE = random.randint(0, sys.maxint)
            csvFilename = 'syn_' + str(SEEDPERFILE) + "_" + str(rowCount) + 'x' + str(colCount) + '.csv'
            csvPathname = SYNDATASETS_DIR + '/' + csvFilename

            print "\nCreating random", csvPathname
            write_syn_dataset(csvPathname, rowCount, colCount, SEEDPERFILE, translateList)

            parseKey = h2o_cmd.parseFile(None, csvPathname, key2=key2, timeoutSecs=30)
            print csvFilename, 'parse time:', parseKey['response']['time']
            print "Parse result['destination_key']:", parseKey['destination_key']

            # We should be able to see the parse result?
            inspect = h2o_cmd.runInspect(None, parseKey['destination_key'])
            print "\n" + csvFilename

            paramDict2 = {}
            for k in paramDict:
                paramDict2[k] = paramDict[k][0]

            y = colCount
            kwargs = {'y': y, 'max_iter': 50, 'case': 1}
            kwargs.update(paramDict2)

            start = time.time()
            glm = h2o_cmd.runGLMOnly(parseKey=parseKey, timeoutSecs=timeoutSecs, **kwargs)
            print "glm end on ", csvPathname, 'took', time.time() - start, 'seconds'
            # only col Y-1 (next to last)doesn't get renamed in coefficients due to enum/categorical expansion
            print "y:", y 
            h2o_glm.simpleCheckGLM(self, glm, None, **kwargs)

            if not h2o.browse_disable:
                h2b.browseJsonHistoryAsUrlLastMatch("GLM")
                time.sleep(15)
                h2b.browseJsonHistoryAsUrlLastMatch("Inspect")
                time.sleep(15)

if __name__ == '__main__':
    h2o.unit_main()
