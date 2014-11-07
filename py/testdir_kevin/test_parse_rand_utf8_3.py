import unittest, random, sys, time, codecs
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_import as h2i

DEBUG = False
UTF8 = True
UTF8_MULTIBYTE = False
ENUM_LENGTH = 10
DISABLE_ALL_NA = True
CAUSE_RANDOM_NA = True

DO_SUMMARY = False # summary slow for some reason

print "Original intent was this, but now it does something else"
print "This focuses on embedding the special h2o bytes into multi-byte UTF8 chars"
print "Brandon says there are no legal UTF8 multi-byte that have legal single byte embedded"
print "that might be true for ASCII, but what about UTF8 single byte? We don't interpret any UTF8"
print "single byte (only ASCII) so we leave that interpretation up to other layers"

print "They will print as some multi-byte UTF8 char. h2o should always parse the right # of cols"
print "I suppose we can guarantee col count if we don't use comma character except"
print "as the col separator. And if we don't use the eol symbols (CR/CRLF/LF)"

# whitespace: tab or space. use
# eol:  don't use: 
# we don't enable single quotes, so those are usable
# double quote: unmatched double quote will change # of cols, so don't use

def massageUTF8Choices(ordinalChoices):
    # is nul acting like lineend?
    if 0x00 in ordinalChoices:
        ordinalChoices.remove(0x00) # nul

    if 0x0d in ordinalChoices:
        ordinalChoices.remove(0x0d) # cr
    if 0x0a in ordinalChoices:
        ordinalChoices.remove(0x0a) # lf

    # if the first line of the dataset starts with # or @ we ignore it
    # means the row count will be wrong. fixed below in rand selection
    
    # ordinalChoices.remove(0x01) # hiveseparator
    # ordinalChoices.remove(0x20) # space
    ordinalChoices.remove(0x22) # double quote BUG: starting row with " and no matching quote causes row drop 

    # ordinalChoices.remove(0x27) # apostrophe. should be legal if single quotes not enabled
    ordinalChoices.remove(0x2c) # comma
    # ordinalChoices.remove(0x3b) # semicolon

    # if we always have another non-digit it there, we don't need to remove digits?
    # we're not checking na counts anyhow
    # if 0x30 in ordinalChoices:
    if 1==0:
        ordinalChoices.remove(0x30) # 0
        ordinalChoices.remove(0x31) # 1
        ordinalChoices.remove(0x32) # 2
        ordinalChoices.remove(0x33) # 3
        ordinalChoices.remove(0x34) # 4
        ordinalChoices.remove(0x35) # 5
        ordinalChoices.remove(0x36) # 6
        ordinalChoices.remove(0x37) # 7
        ordinalChoices.remove(0x38) # 8
        ordinalChoices.remove(0x39) # 9

    if 0x7f in ordinalChoices:
        ordinalChoices.remove(0x7f) # del
    # print ordinalChoices


if UTF8:
    # what about multi-byte UTF8
    ordinalChoices = range(0x0, 0x100) # doesn't include last value ..allow ff
else:  # ascii subset?
    ordinalChoices = range(0x0, 0x80) # doesn't include last value ..allow ff

if UTF8_MULTIBYTE:
    # this full range causes too many unique enums? and we get flipped to NA
    # but if we keep the # rows small, it should be fine
    ordinalChoicesMulti  = range(0x000000,0x00007f) # 1byte
    ordinalChoicesMulti += range(0x000080,0x00009f) # 2byte
    ordinalChoicesMulti += range(0x0000a0,0x0003ff) # 2byte
    ordinalChoicesMulti += range(0x000400,0x0007ff) # 2byte
    ordinalChoicesMulti += range(0x000800,0x003fff) # 3byte
    ordinalChoicesMulti += range(0x004000,0x00ffff) # 3byte
    ordinalChoicesMulti += range(0x010000,0x03ffff) # 3byte
    ordinalChoicesMulti += range(0x040000,0x10ffff) # 4byte

# even if we're doing ascii only!
massageUTF8Choices(ordinalChoices)

if UTF8_MULTIBYTE:
    massageUTF8Choices(ordinalChoicesMulti)

def generate_random_utf8_string(length=1, multi=False, row=0, col=0):
    # want to handle more than 256 numbers
    cList = []
    for i in range(length):
        r = random.choice(ordinalChoicesMulti if multi else ordinalChoices)
        if (row==1 and col==1 and i==0): 
            while (r==0x40 or r==0x23): # @ is 0x40, # is 0x23
                # rechoose
                r = random.choice(ordinalChoicesMulti if multi else ordinalChoices)
        
        # we sholdn't encode it here. Then we wouldn't have to decode it to unicode before writing.
        c = unichr(r).encode('utf-8')
        cList.append(c)
    # this is a random byte string now, of type string?
    return "".join(cList)

def write_syn_dataset(csvPathname, rowCount, colCount=1, scale=1,
        colSepChar=",", rowSepChar="\n", SEED=12345678):
    # always re-init with the same seed. 
    # that way the sequence of random choices from the enum list should stay the same for each call? 
    # But the enum list is randomized
    robj = random.Random(SEED)

    if UTF8 or UTF8_MULTIBYTE:
        dsf = codecs.open(csvPathname, encoding='utf-8', mode='w+')
    else:
        dsf = open(csvPathname, "w+")

    for row in range(rowCount):
        rowData = []
        for col in range(colCount):
            # put in a small number of NAs (1%)
            if not DISABLE_ALL_NA and (CAUSE_RANDOM_NA and robj.randint(0,99)==0): 
                rowData.append('')
            else:
                # pass the row and col so if row 1, col 1, we can avoid start with # or @
                r = generate_random_utf8_string(length=ENUM_LENGTH, multi=UTF8_MULTIBYTE, row=row, col=col)
                rowData.append(r)

        rowDataCsv = colSepChar.join(rowData) + rowSepChar

        if UTF8 or UTF8_MULTIBYTE:
            # decode to unicode
            decoded = rowDataCsv.decode('utf-8')
            if DEBUG:
                # I suppose by having it encoded as utf, we can see the byte representation here?
                print "str:", repr(rowDataCsv), type(rowDataCsv)
                # this has the right length..multibyte utf8 are decoded 
                print "utf8:" , repr(decoded), type(decoded)
            dsf.write(decoded)
        else:
            dsf.write(rowDataCsv)
        
    dsf.close()
    return

class Basic(unittest.TestCase):
    def tearDown(self):
        # h2o.sleep(300)
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = h2o.setup_random_seed()
        h2o.init(3,java_heap_GB=3)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_parse_utf8_3(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()

        if DEBUG:
            n = 20
        else:
            n = 10000
            n = 1000
            n = 500

        # from command line arg -long
        if h2o.long_test_case:
            repeat = 1000 
        else:
            repeat = 50

        scale = 1
        tryList = [
            (n, 3, 'cI', 300), 
            (n, 3, 'cI', 300), 
            (n, 3, 'cI', 300), 
        ]

        for r in range(repeat):
            for (rowCount, colCount, hex_key, timeoutSecs) in tryList:
                SEEDPERFILE = random.randint(0, sys.maxint)
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
                write_syn_dataset(csvPathname, rowCount, colCount, scale=1,
                    colSepChar=colSepChar, rowSepChar=rowSepChar, SEED=SEEDPERFILE)
                elapsed = time.time() - start
                print "took %s seconds to create %s" % (elapsed, csvPathname)

                parseResult = h2i.import_parse(path=csvPathname, schema='put', hex_key=hex_key, header=0,
                    timeoutSecs=60, separator=colSepInt, doSummary=DO_SUMMARY)
                print "Parse result['destination_key']:", parseResult['destination_key']
                
                inspect = h2o_cmd.runInspect(key=parseResult['destination_key'])
                numCols = inspect['numCols']
                numRows = inspect['numRows']

                h2o_cmd.infoFromInspect(inspect)

                # Each column should get .10 random NAs per iteration. Within 10%? 
                missingValuesList = h2o_cmd.infoFromInspect(inspect)
                # print "missingValuesList", missingValuesList
                # for mv in missingValuesList:
                #     self.assertAlmostEqual(mv, expectedNA, delta=0.1 * mv, 
                #        msg='mv %s is not approx. expected %s' % (mv, expectedNA))

                # might have extra rows
                if numRows!=rowCount:
                    raise Exception("Expect numRows %s = rowCount %s because guaranteed not to have extra eols" % \
                        (numRows, rowCount))
                # numCols should be right?
                self.assertEqual(colCount, numCols)

                (missingValuesDict, constantValuesDict, enumSizeDict, colTypeDict, colNameDict) = \
                    h2o_cmd.columnInfoFromInspect(parseResult['destination_key'], 
                    exceptionOnMissingValues=False)

if __name__ == '__main__':
    h2o.unit_main()
