import unittest, random, sys, time, re, math
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_hosts, h2o_browse as h2b, h2o_import as h2i, h2o_rf, h2o_util, h2o_gbm


SPEEDRF = True
MULTINOMIAL = 3
DO_WITH_INT = False
# use randChars for the random chars to use
def random_enum(randChars, maxEnumSize):
    choiceStr = randChars
    r = ''.join(random.choice(choiceStr) for x in range(maxEnumSize))
    return r

def create_enum_list(randChars="abcd", maxEnumSize=8, listSize=10):
    if DO_WITH_INT:
        enumList = range(listSize)
    else:
        enumList = [random_enum(randChars, random.randint(2,maxEnumSize)) for i in range(listSize)]
    return enumList

def write_syn_dataset(csvPathname, enumList, rowCount, colCount=1,
        colSepChar=",", rowSepChar="\n", SEED=12345678):

    # always re-init with the same seed. that way the sequence of random choices from the enum list should stay the same
    # for each call? But the enum list is randomized
    robj = random.Random(SEED)
    dsf = open(csvPathname, "w+")
    for row in range(rowCount):
        rowData = []
        # keep a sum of all the index mappings for the enum chosen (for the features in a row)
        riIndexSum = 0
        for col in range(colCount):
            riIndex = robj.randint(0, len(enumList)-1)
            rowData.append(enumList[riIndex])
            riIndexSum += riIndex

        # output column
        # make the output column match odd/even row mappings.
        # change...make it 1 if the sum of the enumList indices used is odd
        ri = riIndexSum % MULTINOMIAL
        rowData.append(ri)
        rowDataCsv = colSepChar.join(map(str,rowData)) + rowSepChar
        dsf.write(rowDataCsv)
    dsf.close()

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED, localhost
        SEED = h2o.setup_random_seed()
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(1,java_heap_GB=1)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        ### time.sleep(3600)
        h2o.tear_down_cloud()

    def test_rf_enums_mappings_fvec(self):
        h2o.beta_features = True
        SYNDATASETS_DIR = h2o.make_syn_dir()

        n = 3000
        tryList = [
            # (n, 1, 'cD', 300), 
            # (n, 2, 'cE', 300), 
            # (n, 3, 'cF', 300), 
            # (n, 4, 'cG', 300), 
            # (n, 5, 'cH', 300), 
            # (n, 6, 'cI', 300), 
            (n, 3, 'cI', 300), 
            (n, 3, 'cI', 300), 
            (n, 3, 'cI', 300), 
            ]

        # SEED_FOR_TRAIN = random.randint(0, sys.maxint)
        SEED_FOR_TRAIN = 1234567890
        SEED_FOR_SCORE = 9876543210
        errorHistory = []
        enumHistory = []

        for (rowCount, colCount, hex_key, timeoutSecs) in tryList:
            enumList = create_enum_list(listSize=10)
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
            write_syn_dataset(csvPathname, enumList, rowCount, colCount, 
                colSepChar=colSepChar, rowSepChar=rowSepChar, SEED=SEED_FOR_TRAIN)

            print "Creating random", csvScorePathname, "for rf scoring with prior model (using same enum list)"
            # same enum list/mapping, but different dataset?
            write_syn_dataset(csvScorePathname, enumListForScore, rowCount, colCount, 
                colSepChar=colSepChar, rowSepChar=rowSepChar, SEED=SEED_FOR_SCORE)

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

            if SPEEDRF:
                kwargs = {
                    'destination_key': modelKey,
                    'response': y,
                    'num_trees': 1,
                    'max_depth': 100,
                    'oobee': 1,
                    'seed': 123456789,
                }
            else:
                kwargs = {
                    'destination_key': modelKey,
                    'response': y,
                    'classification': 1,
                    'ntrees': 1,
                    'max_depth': 100,
                    'validation': scoreDataKey,
                    'seed': 123456789,
                }

            for r in range(4):
                start = time.time()
                
                if SPEEDRF:
                    rfResult = h2o_cmd.runSpeeDRF(parseResult=parseResult, 
                        timeoutSecs=timeoutSecs, pollTimeoutSecs=180, **kwargs)
                else:
                    rfResult = h2o_cmd.runRF(parseResult=parseResult, 
                        timeoutSecs=timeoutSecs, pollTimeoutSecs=180, **kwargs)
                
                print "rf end on ", parseResult['destination_key'], 'took', time.time() - start, 'seconds'
                # print h2o.dump_json(rfResult)
                (classification_error, classErrorPctList, totalScores) = h2o_rf.simpleCheckRFView(rfv=rfResult)
                h2o_cmd.runScore(dataKey=scoreDataKey, modelKey=modelKey, vactual=y, vpredict=1, doAUC=not MULTINOMIAL) # , expectedAuc=0.5)
                
                errorHistory.append(classification_error)
                enumHistory.append(enumList)

            print "error from all runs on this dataset (with different enum mappings)"
            print errorHistory
            for e in enumHistory:
                print e


if __name__ == '__main__':
    h2o.unit_main()
