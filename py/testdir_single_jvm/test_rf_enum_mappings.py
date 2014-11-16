import unittest, random, sys, time, re, math
sys.path.extend(['.','..','../..','py'])

import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i, h2o_rf, h2o_util, h2o_gbm
import itertools



PRODUCT_SEQ = True
MULTIPLIER = 1

COLS = 3
ROWS = 300
SPEEDRF = False
GBM = False
MULTINOMIAL = 2
DO_WITH_INT = False
ENUMS = 100

# remember since the output is parity, that only 3 values out of the 6 affect the output
# so 3 * 3 * 3 = 27 leaves min? for 3 cols
# ENUMLIST = ['bacaa', 'cbcbcacd', 'dccdbda', 'efg', 'hij', 'jkl']
ENUMLIST = ['a', 'b', 'c', 'd', 'e', 'f']
# ENUMLIST = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j']

# ENUMLIST = ['efg', 'hij', 'jkl']
# ENUMLIST = ['bacaa', 'efg', 'hij', 'jkl']
# ENUMLIST = ['bacaa', 'cbcbcacd', 'efg', 'hij', 'jkl']
# use randChars for the random chars to use
def random_enum(randChars, maxEnumSize):
    choiceStr = randChars
    r = ''.join(random.choice(choiceStr) for x in range(maxEnumSize))
    return r

def create_enum_list(randChars="abcd", maxEnumSize=8, listSize=1000):

    if DO_WITH_INT:
        if ENUMLIST:
            enumList = range(len(ENUMLIST))
        else:
            enumList = range(listSize)
    else:
        if ENUMLIST:
            enumList = ENUMLIST
        else:
            enumList = [random_enum(randChars, random.randint(2, maxEnumSize)) for i in range(listSize)]

    return enumList

def write_syn_dataset(csvPathname, enumList, rowCount, colCount=1,
        colSepChar=",", rowSepChar="\n", SEED=12345678):

    # always re-init with the same seed. that way the sequence of random choices from the enum list should stay the same
    # for each call? But the enum list is randomized
    robj = random.Random(SEED)
    dsf = open(csvPathname, "w+")

    # just one of all permutations: ignore row count
    if PRODUCT_SEQ:
        indexList = range(len(ENUMLIST))
        rowIndexList = list(itertools.product(indexList, repeat=COLS))
        # do it 10x!
        rowsToDo = MULTIPLIER * len(rowIndexList)
    else:
        rowsToDo = rowCount

    
    for row in range(rowsToDo):
        if PRODUCT_SEQ:
            rowIndex = rowIndexList[row % len(rowIndexList)]
        else:
            rowIndex = [robj.randint(0, len(enumList)-1) for col in range(colCount)]

        # keep a sum of all the index mappings for the enum chosen (for the features in a row)
        riIndexSum  = sum(rowIndex)

        rowData = [enumList[riIndex] for riIndex in rowIndex]

        # output column
        # make the output column match odd/even row mappings.
        # change...make it 1 if the sum of the enumList indices used is odd
        ri = riIndexSum % MULTINOMIAL
        rowData.append(ri)
        rowDataCsv = colSepChar.join(map(str,rowData)) + rowSepChar
        dsf.write(rowDataCsv)

    dsf.close()
    rowIndexCsv = colSepChar.join(map(str,rowIndex)) + rowSepChar
    return rowIndexCsv # last line as index

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

    def test_rf_enums_mappings(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()

        tryList = [
            # (n, 1, 'cD', 300), 
            # (n, 2, 'cE', 300), 
            # (n, 3, 'cF', 300), 
            # (n, 4, 'cG', 300), 
            # (n, 5, 'cH', 300), 
            # (n, 6, 'cI', 300), 
            (ROWS, COLS, 'cI', 300), 
            (ROWS, COLS, 'cI', 300), 
            (ROWS, COLS, 'cI', 300), 
            ]

        # SEED_FOR_TRAIN = random.randint(0, sys.maxint)
        SEED_FOR_TRAIN = 1234567890
        SEED_FOR_SCORE = 9876543210
        errorHistory = []
        enumHistory = []
        lastcolsTrainHistory = []
        lastcolsScoreHistory = []

        for (rowCount, colCount, hex_key, timeoutSecs) in tryList:
            enumList = create_enum_list(listSize=ENUMS)
            # reverse the list
            enumList.reverse()

            # using the comma is nice to ensure no craziness
            colSepHexString = '2c' # comma
            colSepChar = colSepHexString.decode('hex')
            colSepInt = int(colSepHexString, base=16)
            print "colSepChar:", colSepChar

            rowSepHexString = '0a' # newline
            rowSepChar = rowSepHexString.decode('hex')
            print "rowSepChar:", rowSepChar

            csvFilename = 'syn_enums_' + str(rowCount) + 'x' + str(colCount) + '.csv'
            csvPathname = SYNDATASETS_DIR + '/' + csvFilename
            csvScoreFilename = 'syn_enums_score_' + str(rowCount) + 'x' + str(colCount) + '.csv'
            csvScorePathname = SYNDATASETS_DIR + '/' + csvScoreFilename

            # use same enum List
            enumListForScore = enumList

            print "Creating random", csvPathname, "for rf model building"
            lastcols = write_syn_dataset(csvPathname, enumList, rowCount, colCount, 
                colSepChar=colSepChar, rowSepChar=rowSepChar, SEED=SEED_FOR_TRAIN)

            lastcolsTrainHistory.append(lastcols)

            print "Creating random", csvScorePathname, "for rf scoring with prior model (using same enum list)"
            # same enum list/mapping, but different dataset?
            lastcols = write_syn_dataset(csvScorePathname, enumListForScore, rowCount, colCount, 
                colSepChar=colSepChar, rowSepChar=rowSepChar, SEED=SEED_FOR_SCORE)
            lastcolsScoreHistory.append(lastcols)

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
            # limit depth and number of trees to accentuate the issue with categorical split decisions

            # use mtries so both look at all cols at every split? doesn't matter for speedrf
            # does speedrf try one more time? with 3 cols, mtries=2, so another try might 
            # get a look at the missing col
            # does matter for drf2. does it "just stop"
            # trying mtries always looking at all columns or 1 col might be interesting
            if SPEEDRF:
                kwargs = {
                    'sample_rate': 0.999,
                    'destination_key': modelKey,
                    'response': y,
                    'ntrees': 1,
                    'max_depth': 100,
                    # 'oobee': 1,
                    'validation': hex_key,
                    # 'validation': scoreDataKey,
                    'seed': 123456789,
                    'mtries': COLS,
                }
            elif GBM:
                kwargs = {
                    'destination_key': modelKey,
                    'response': y,
                    'validation': scoreDataKey,
                    'seed': 123456789,
                    # 'learn_rate': .1,
                    'ntrees': 1,
                    'max_depth': 100,
                    'min_rows': 1,
                    'classification': 1,
                }
            else:
                kwargs = {
                    'sample_rate': 0.999,
                    'destination_key': modelKey,
                    'response': y,
                    'classification': 1,
                    'ntrees': 1,
                    'max_depth': 100,
                    'min_rows': 1,
                    'validation': hex_key,
                    # 'validation': scoreDataKey,
                    'seed': 123456789,
                    'nbins': 1024,
                    'mtries': COLS,
                }

            for r in range(2):
                start = time.time()

                if GBM:
                    gbmResult = h2o_cmd.runGBM(parseResult=parseResult,
                        timeoutSecs=timeoutSecs, pollTimeoutSecs=180, **kwargs)

                    print "gbm end on ", parseResult['destination_key'], 'took', time.time() - start, 'seconds'
                    # print h2o.dump_json(gbmResult)
                    (classification_error, classErrorPctList, totalScores) = h2o_gbm.simpleCheckGBMView(gbmv=gbmResult)
                
                elif SPEEDRF:
                    rfResult = h2o_cmd.runSpeeDRF(parseResult=parseResult, 
                        timeoutSecs=timeoutSecs, pollTimeoutSecs=180, **kwargs)
                    print "speedrf end on ", parseResult['destination_key'], 'took', time.time() - start, 'seconds'
                    (classification_error, classErrorPctList, totalScores) = h2o_rf.simpleCheckRFView(rfv=rfResult)

                else:
                    rfResult = h2o_cmd.runRF(parseResult=parseResult, 
                        timeoutSecs=timeoutSecs, pollTimeoutSecs=180, **kwargs)
                    print "rf end on ", parseResult['destination_key'], 'took', time.time() - start, 'seconds'
                    (classification_error, classErrorPctList, totalScores) = h2o_rf.simpleCheckRFView(rfv=rfResult)
                
                h2o_cmd.runScore(dataKey=scoreDataKey, modelKey=modelKey, vactual=y, vpredict=1, doAUC=not MULTINOMIAL) # , expectedAuc=0.5)
                
                errorHistory.append(classification_error)
                enumHistory.append(enumList)

            print "error from all runs on this dataset (with different enum mappings)"
            print errorHistory
            for e in enumHistory:
                print e

            print "last row from all train datasets, as integer"
            for l in lastcolsTrainHistory:
                print l
            print "last row from all score datasets, as integer"
            for l in lastcolsScoreHistory:
                print l

if __name__ == '__main__':
    h2o.unit_main()
