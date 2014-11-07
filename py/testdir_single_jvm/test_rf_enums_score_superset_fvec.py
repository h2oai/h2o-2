import unittest, random, sys, time, re, math
sys.path.extend(['.','..','../..','py'])

import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i, h2o_rf, h2o_util, h2o_gbm

# use randChars for the random chars to use
def random_enum(randChars, maxEnumSize):
    choiceStr = randChars
    r = ''.join(random.choice(choiceStr) for x in range(maxEnumSize))
    return r

def create_enum_list(randChars="abcd", maxEnumSize=8, listSize=10):
    enumList = [random_enum(randChars, random.randint(2,maxEnumSize)) for i in range(listSize)]
    return enumList

def write_syn_dataset(csvPathname, enumList, rowCount, colCount=1, SEED='12345678', 
        colSepChar=",", rowSepChar="\n"):
    r1 = random.Random(SEED)
    dsf = open(csvPathname, "w+")
    for row in range(rowCount):
        rowData = []
        for col in range(colCount):
            ri = random.choice(enumList)
            rowData.append(ri)

        # output column
        # ri = r1.randint(0,1)
        # skew the binomial 0,1 distribution. (by rounding to 0 or 1
        ri = round(r1.triangular(0,1,0.3), 0)
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
        SEED = h2o.setup_random_seed()
        h2o.init(1,java_heap_GB=1)

    @classmethod
    def tearDownClass(cls):
        ### time.sleep(3600)
        h2o.tear_down_cloud()

    def test_rf_enums_score_superset_fvec(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()

        n = 3000
        tryList = [
            (n, 1, 'cD', 300), 
            (n, 2, 'cE', 300), 
            (n, 3, 'cF', 300), 
            (n, 4, 'cG', 300), 
            (n, 5, 'cH', 300), 
            (n, 6, 'cI', 300), 
            ]

        for (rowCount, colCount, hex_key, timeoutSecs) in tryList:
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
            csvScoreFilename = 'syn_enums_score_' + str(rowCount) + 'x' + str(colCount) + '.csv'
            csvScorePathname = SYNDATASETS_DIR + '/' + csvScoreFilename

            enumList = create_enum_list(listSize=10)
            # use half of the enums for creating the scoring dataset
            enumListForScore = random.sample(enumList,5)

            # add a extra enum for scoring that's not in the model enumList
            enumListForScore.append("xyzzy")

            print "Creating random", csvPathname, "for rf model building"
            write_syn_dataset(csvPathname, enumList, rowCount, colCount, SEEDPERFILE, 
                colSepChar=colSepChar, rowSepChar=rowSepChar)

            print "Creating random", csvScorePathname, "for rf scoring with prior model (using enum subset)"
            write_syn_dataset(csvScorePathname, enumListForScore, rowCount, colCount, SEEDPERFILE, 
                colSepChar=colSepChar, rowSepChar=rowSepChar)

            scoreDataKey = "score_" + hex_key
            parseResult = h2i.import_parse(path=csvScorePathname, schema='put', hex_key=scoreDataKey, 
                timeoutSecs=30, separator=colSepInt)

            parseResult = h2i.import_parse(path=csvPathname, schema='put', hex_key=hex_key,
                timeoutSecs=30, separator=colSepInt)
            print "Parse result['destination_key']:", parseResult['destination_key']

            print "\n" + csvFilename
            (missingValuesDict, constantValuesDict, enumSizeDict, colTypeDict, colNameDict) = \
                h2o_cmd.columnInfoFromInspect(parseResult['destination_key'], exceptionOnMissingValues=True)

            y = colCount
            modelKey = 'enums'
            ntrees = 5
            kwargs = {
                'destination_key': modelKey,
                'response': y,
                'classification': 1,
                'ntrees': ntrees,
                'validation': scoreDataKey,
            }

            start = time.time()
            rfResult = h2o_cmd.runRF(parseResult=parseResult, timeoutSecs=timeoutSecs, pollTimeoutSecs=180, **kwargs)
            print "rf end on ", parseResult['destination_key'], 'took', time.time() - start, 'seconds'
            (classification_error, classErrorPctList, totalScores) = h2o_rf.simpleCheckRFView(rfv=rfResult, ntree=ntrees)
            predictKey = 'Predict.hex'
            h2o_cmd.runScore(dataKey=scoreDataKey, modelKey=modelKey, vactual=y, vpredict=1, expectedAuc=0.5)


if __name__ == '__main__':
    h2o.unit_main()
