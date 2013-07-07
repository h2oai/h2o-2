import unittest
import random, sys, time, os
import string
import re
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_hosts, h2o_browse as h2b, h2o_import as h2i, h2o_glm, h2o_util

# use randChars for the random chars to use
def random_enum(maxEnumSize, randChars="abcd"):
    choiceStr = randChars
    mightBeNumberOrWhite = True
    while mightBeNumberOrWhite:
        r = ''.join(random.choice(choiceStr) for x in range(maxEnumSize))
        mightBeNumberOrWhite = h2o_util.might_h2o_think_number_or_whitespace(r)
    return r

def create_enum_list(maxEnumSize=8, listSize=11000, **kwargs):
    enumList = [random_enum(random.randint(2,maxEnumSize), **kwargs) for i in range(listSize)]
    return enumList

def write_syn_dataset(csvPathname, rowCount, colCount=1, SEED='12345678', 
        colSepChar=",", rowSepChar="\n"):
    r1 = random.Random(SEED)
    enumList = create_enum_list()

    dsf = open(csvPathname, "w+")
    for row in range(rowCount):
        rowData = []
        for col in range(colCount):
            ri = random.choice(enumList)
            rowData.append(ri)

        # output column
        ri = r1.randint(0,1)
        rowData.append(ri)
        rowDataCsv = colSepChar.join(map(str,rowData)) + rowSepChar
        dsf.write(rowDataCsv)
    dsf.close()

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
        global localhost
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(1,java_heap_GB=1)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        ### time.sleep(3600)
        h2o.tear_down_cloud()

    def test_GLM_many_enums(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()

        n = 200
        tryList = [
            (n, 1, 'cD', 300), 
            (n, 2, 'cE', 300), 
            (n, 3, 'cF', 300), 
            (n, 4, 'cG', 300), 
            (n, 5, 'cH', 300), 
            (n, 6, 'cI', 300), 
            ]

        for (rowCount, colCount, key2, timeoutSecs) in tryList:
            # using the comma is nice to ensure no craziness
            colSepHexString = '2c' # comma
            colSepChar = colSepHexString.decode('hex')
            colSepInt = int(colSepHexString, base=16)
            print "colSepChar:", colSepChar

            rowSepHexString = '0a' # newline
            rowSepChar = rowSepHexString.decode('hex')
            print "rowSepChar:", rowSepChar

            SEEDPERFILE = random.randint(0, sys.maxint)
            csvFilename = 'syn_enums_' + str(rowCount) + 'x' + str(colCount) + '.csv'
            csvPathname = SYNDATASETS_DIR + '/' + csvFilename

            print "Creating random", csvPathname
            write_syn_dataset(csvPathname, rowCount, colCount, SEEDPERFILE, 
                colSepChar=colSepChar, rowSepChar=rowSepChar)

            parseKey = h2o_cmd.parseFile(None, csvPathname, key2=key2, 
                timeoutSecs=30, separator=colSepInt)
            print csvFilename, 'parse time:', parseKey['response']['time']
            print "Parse result['destination_key']:", parseKey['destination_key']

            print "\n" + csvFilename
            missingValuesDict = h2o_cmd.check_enums_from_inspect(parseKey)
            if missingValuesDict:
                m = [str(k) + ":" + str(v) for k,v in missingValuesDict.iteritems()]
                raise Exception("Looks like columns got flipped to NAs: " + ", ".join(m))

            y = colCount
            kwargs = {'y': y, 'max_iter': 1, 'n_folds': 1, 'alpha': 0.2, 'lambda': 1e-5, 
                'case_mode': '=', 'case': 0}
            start = time.time()
            glm = h2o_cmd.runGLMOnly(parseKey=parseKey, timeoutSecs=timeoutSecs, pollTimeoutSecs=180, **kwargs)
            print "glm end on ", parseKey['destination_key'], 'took', time.time() - start, 'seconds'

            h2o_glm.simpleCheckGLM(self, glm, None, **kwargs)

            GLMModel = glm['GLMModel']
            modelKey = GLMModel['model_key']

            start = time.time()
            # score with same dataset (will change to recreated dataset with one less enum
            scoreDataKey = parseKey['destination_key']
            glmScore = h2o_cmd.runGLMScore(key=scoreDataKey,
                model_key=modelKey, thresholds="0.5", timeoutSecs=timeoutSecs)
            print "glmScore end on ", scoreDataKey, 'took', time.time() - start, 'seconds'
            ### print h2o.dump_json(glmScore)
            classErr = glmScore['validation']['classErr']
            auc = glmScore['validation']['auc']
            err = glmScore['validation']['err']
            print "classErr:", classErr
            print "err:", err
            print "auc:", auc

if __name__ == '__main__':
    h2o.unit_main()
