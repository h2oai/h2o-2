import unittest, random, sys, time, re, math
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i, h2o_glm
import h2o_util, h2o_browse as h2b, h2o_gbm

# use randChars for the random chars to use
def random_enum(randChars, maxEnumSize):
    choiceStr = randChars
    r = ''.join(random.choice(choiceStr) for x in range(maxEnumSize))
    return r

ONE_RATIO = 100
ENUM_RANGE = 20
MAX_ENUM_SIZE = 4
GAUSS_ENUMS = True
def create_enum_list(randChars="012345679", maxEnumSize=MAX_ENUM_SIZE, listSize=ENUM_RANGE):
    # okay to have duplicates?
    enumList = [random_enum(randChars, random.randint(2,maxEnumSize)) for i in range(listSize)]
    # enumList = [random_enum(randChars, maxEnumSize) for i in range(listSize)]
    return enumList

def write_syn_dataset(csvPathname, enumList, rowCount, colCount=1, SEED='12345678', 
        colSepChar=",", rowSepChar="\n"):
    enumRange = len(enumList)
    r1 = random.Random(SEED)
    dsf = open(csvPathname, "w+")
    for row in range(rowCount):
        rowData = []
        for col in range(colCount):
            if GAUSS_ENUMS:
                # truncated gaussian distribution, from the enumList
                value = None
                while not value:
                    value = int(random.gauss(enumRange/2, enumRange/4))
                    if  value < 0 or value >= enumRange:
                        value = None
                rowData.append(enumList[value])
            else:
                value = random.choice(enumList)
                rowData.append(value)

        # output column
        # ri = r1.randint(0,1)
        # skew the binomial 0,1 distribution. (by rounding to 0 or 1
        # ri = round(r1.triangular(0,1,0.3), 0)
        # just put a 1 in every 100th row
        if (row % ONE_RATIO)==0:
            ri = 1
        else:
            ri = 0
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
        h2o.tear_down_cloud()

    def test_GLM2_ints_unbalanced(self):
        ### h2b.browseTheCloud()
        SYNDATASETS_DIR = h2o.make_syn_dir()

        n = 2000
        tryList = [
            (n, 1, 'cD', 300), 
            (n, 2, 'cE', 300), 
            (n, 4, 'cF', 300), 
            (n, 8, 'cG', 300), 
            (n, 16, 'cH', 300), 
            (n, 32, 'cI', 300), 
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

            enumList = create_enum_list()
            # use half of the enums for creating the scoring dataset
            enumListForScore = random.sample(enumList,5)

            print "Creating random", csvPathname, "for glm model building"
            write_syn_dataset(csvPathname, enumList, rowCount, colCount, SEEDPERFILE, 
                colSepChar=colSepChar, rowSepChar=rowSepChar)

            print "Creating random", csvScorePathname, "for glm scoring with prior model (using enum subset)"
            write_syn_dataset(csvScorePathname, enumListForScore, rowCount, colCount, SEEDPERFILE, 
                colSepChar=colSepChar, rowSepChar=rowSepChar)

            parseResult = h2i.import_parse(path=csvPathname, schema='put', hex_key=hex_key, 
                timeoutSecs=30, separator=colSepInt)
            print "Parse result['destination_key']:", parseResult['destination_key']

            print "\n" + csvFilename
            (missingValuesDict, constantValuesDict, enumSizeDict, colTypeDict, colNameDict) = \
                h2o_cmd.columnInfoFromInspect(parseResult['destination_key'], exceptionOnMissingValues=True)

            y = colCount
            modelKey = 'xyz'
            kwargs = {
                'n_folds': 0,
                'destination_key': modelKey,
                'response': y, 
                'max_iter': 200, 
                'family': 'binomial',
                'alpha': 0, 
                'lambda': 0, 
                }

            start = time.time()

            updateList= [ 
                {'alpha': 0.5, 'lambda': 1e-5},
                # {'alpha': 0.25, 'lambda': 1e-4},
            ]


            # Try each one
            for updateDict in updateList:
                print "\n#################################################################"
                print updateDict
                kwargs.update(updateDict)
                glm = h2o_cmd.runGLM(parseResult=parseResult, timeoutSecs=timeoutSecs, pollTimeoutSecs=180, **kwargs)
                print "glm end on ", parseResult['destination_key'], 'took', time.time() - start, 'seconds'

                h2o_glm.simpleCheckGLM(self, glm, None, **kwargs)

                parseResult = h2i.import_parse(path=csvScorePathname, schema='put', hex_key="B.hex",
                    timeoutSecs=30, separator=colSepInt)

                h2o_cmd.runScore(dataKey="B.hex", modelKey=modelKey, 
                    vactual='C' + str(y+1), vpredict=1, expectedAuc=0.50, expectedAucTol=0.20)


if __name__ == '__main__':
    h2o.unit_main()
