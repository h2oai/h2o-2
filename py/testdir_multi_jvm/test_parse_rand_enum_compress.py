import unittest, random, sys, time, re, math
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_hosts, h2o_browse as h2b, h2o_import as h2i, h2o_rf, h2o_util, h2o_gbm


DO_SUMMARY = False # summary slow for some reason
DISABLE_ALL_NA = False
CAUSE_RANDOM_NA = True
DO_WITH_INT = False
REPORT_OUTPUT = False
REPORT_LAST_ENUM_INDICES = False
MULTINOMIAL = 2
ENUMS_NUM = 20000
# ENUMLIST = ['bacaa', 'cbcbcacd', 'dccdbda', 'efg', 'hij', 'jkl']
ENUMLIST = None

# use randChars for the random chars to use
def random_enum(randChars, maxEnumSize):
    choiceStr = randChars
    r = ''.join(random.choice(choiceStr) for x in range(maxEnumSize))
    return r

# FIX! make these random UTF8 chars
def create_enum_list(randChars="abcdefghijklmnopqrstuvwxyz", maxEnumSize=4, listSize=10):
    if DO_WITH_INT:
        enumList = range(listSize)
    else:
        if ENUMLIST:
            enumList = ENUMLIST
        else:
            enumList = [random_enum(randChars, random.randint(2,maxEnumSize)) for i in range(listSize)]

    return enumList

def write_syn_dataset(csvPathname, enumList, rowCount, colCount=1, scale=1,
        colSepChar=",", rowSepChar="\n", SEED=12345678):
    # always re-init with the same seed. 
    # that way the sequence of random choices from the enum list should stay the same for each call? 
    # But the enum list is randomized
    robj = random.Random(SEED)
    dsf = open(csvPathname, "w+")
    for row in range(rowCount):
        # add some robj choices here, to get more robjness over time with multiple test runs

        rowModulo = row % 1000000 # max range in this if/elif thing

        # only two compression schemes. Well 4
        # NewChunk: uncompressed
        # C1: up to 255
        # C2: up till we force NA
        # int?: when we flip to all NA
        if rowModulo < (100000 * scale):
            # 0 will always force NA, regardless of CAUSE_RANDOM_NA
            howManyEnumsToUse = robj.choice(
                [0 if not DISABLE_ALL_NA else 1, 1]) # zero will be all cause all NA
        elif rowModulo < (200000 * scale):
            howManyEnumsToUse = robj.choice([1,2,3])
        elif rowModulo < (300000 * scale):
            howManyEnumsToUse = robj.choice([4,5])
        elif rowModulo < (400000 * scale):
            howManyEnumsToUse = robj.choice([8,9])
        elif rowModulo < (500000 * scale):
            howManyEnumsToUse = robj.choice([15,16])
        elif rowModulo < (600000 * scale):
            howManyEnumsToUse = robj.choice([31,32])
        elif rowModulo < (700000 * scale):
            howManyEnumsToUse = robj.choice([63,64])
        elif rowModulo < (800000 * scale):
            howManyEnumsToUse = robj.choice([254,255,256,257, 10000])
        else:
            # some primes
            howManyEnumsToUse = robj.choice([1,2,4,8,16,256,10000])

        # never try to use more enums then in the list
        if howManyEnumsToUse > len(enumList):
            print "WARNING: you should make ENUMS_NUM %s bigger than howManyEnumsToUse: %s" % \
                (ENUMS_NUM, howManyEnumsToUse)
            howManyEnumsToUse = len(enumList)

        rowData = []
        # keep a list of the enum indices used..return that for comparing multiple datasets
        # we only need to compare the last one..if it matches, then we probably did the right
        # thing with random seeds
        rowIndex = []
        # keep a sum of all the index mappings for the enum chosen (for the features in a row)
        # use this to calcuate a output (that's dependent on inputs in some repeatable way)
        riIndexSum = 0
        for col in range(colCount):
            # put in a small number of NAs (1%)
            if not DISABLE_ALL_NA and (   
                    (CAUSE_RANDOM_NA and robj.randint(0,99)==0) or 
                    howManyEnumsToUse==0):
                riIndex = None
                riIndexSum += 0 # don't change
                rowData.append('')
            else:
                riIndex = robj.randint(0, howManyEnumsToUse-1)
                if REPORT_OUTPUT:
                    riIndexSum += riIndex

                rowData.append(enumList[riIndex])
                if REPORT_LAST_ENUM_INDICES:
                    rowIndex.append(riIndex)

        # output column
        # make the output column match odd/even row mappings.
        # change...make it 1 if the sum of the enumList indices used is odd
        if REPORT_OUTPUT:
            ri = riIndexSum % MULTINOMIAL
            rowData.append(ri)

        rowDataCsv = colSepChar.join(map(str,rowData)) + rowSepChar
        dsf.write(rowDataCsv)
    dsf.close()
    # this is for comparing whether two datasets were generated identically 
    # (last row is essentially a checksum, given the use of random generator for prior rows)

    # will be empty if we didn't enable REPORT_LAST_ENUM_INDICES
    rowIndexCsv = colSepChar.join(map(str,rowIndex)) + rowSepChar
    return rowIndexCsv 

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED, localhost
        SEED = h2o.setup_random_seed()
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(3,java_heap_GB=3)
        else:
            # force 2 jvms per host!
            h2o_hosts.build_cloud_with_hosts(2, java_heap_GB=10)

    @classmethod
    def tearDownClass(cls):
        ### time.sleep(3600)
        h2o.tear_down_cloud()

    def test_parse_rand_enum_compress(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()

        n = 1000000
        if h2o.long_test_case:
            repeat = 1000 
            scale = 100 # scale up the # of rows
            tryList = [
                (n*scale, 3, 'cI', 300), 
                (n*scale, 3, 'cI', 300), 
                (n*scale, 3, 'cI', 300), 
            ]
        else:
            repeat = 1
            scale = 1
            tryList = [
                (n, 3, 'cI', 300), 
                (n, 3, 'cI', 300), 
                (n, 3, 'cI', 300), 
            ]

        NEW_SEED = random.randint(0, sys.maxint)
        lastcolsHistory = []

        enumList = create_enum_list(listSize=ENUMS_NUM)

        for r in range(repeat):
            for (rowCount, colCount, hex_key, timeoutSecs) in tryList:
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

                print "Creating random", csvPathname
                # same enum list/mapping, but different dataset?
                start = time.time()
                lastcols = write_syn_dataset(csvPathname, enumList, rowCount, colCount, scale=1,
                    colSepChar=colSepChar, rowSepChar=rowSepChar, SEED=NEW_SEED)
                elapsed = time.time() - start
                print "took %s seconds to create %s" % (elapsed, csvPathname)
                # why are we saving this?
                lastcolsHistory.append(lastcols)

                parseResult = h2i.import_parse(path=csvPathname, schema='put', hex_key=hex_key,
                    timeoutSecs=30, separator=colSepInt, doSummary=DO_SUMMARY)
                print "Parse result['destination_key']:", parseResult['destination_key']
                
                inspect = h2o_cmd.runInspect(key=parseResult['destination_key'])
                numCols = inspect['numCols']
                numRows = inspect['numRows']

                h2o_cmd.infoFromInspect(inspect)
                expectedNA = .06 * numRows

                # Each column should get .10 random NAs per iteration. Within 10%? 
                missingValuesList = h2o_cmd.infoFromInspect(inspect)
                # print "missingValuesList", missingValuesList
                for mv in missingValuesList:
                    # h2o_util.assertApproxEqual(mv, expectedMissing, tol=0.01, msg='mv %s is not approx. expected %s' % (mv, expectedMissing))
                    self.assertAlmostEqual(mv, expectedNA, delta=0.1 * mv, 
                        msg='mv %s is not approx. expected %s' % (mv, expectedNA))

                self.assertEqual(rowCount, numRows)
                self.assertEqual(colCount, numCols)

                (missingValuesDict, constantValuesDict, enumSizeDict, colTypeDict, colNameDict) = \
                    h2o_cmd.columnInfoFromInspect(parseResult['destination_key'], 
                    exceptionOnMissingValues=DISABLE_ALL_NA)

if __name__ == '__main__':
    h2o.unit_main()
