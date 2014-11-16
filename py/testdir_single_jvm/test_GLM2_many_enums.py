import unittest, random, sys, time, re
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i, h2o_glm, h2o_util

# details:
# Apparently we don't have any new EOL separators for hive?, just new column separator
# We allow extra chars in the hive separated columns..i.e. single and double quote.

# we want to seed a random dictionary for our enums
# python has some things we can use
# string.ascii_uppercase string.printable string.letters string.digits string.punctuation string.whitespace
# restricting the choices makes it easier to find the bad cases
def random_enum(maxEnumSize, randChars="abeE01" + "$%+-.;|\t ", quoteChars="\'\""):
    choiceStr = randChars + quoteChars
    mightBeNumberOrWhite = True
    while mightBeNumberOrWhite:
        # H2O doesn't seem to tolerate random single or double quote in the first two rows.
        # disallow that by not passing quoteChars for the first two rows (in call to here)
        r = ''.join(random.choice(choiceStr) for x in range(maxEnumSize))
        mightBeNumberOrWhite = h2o_util.might_h2o_think_number_or_whitespace(r)
    return r

# MAX_ENUM_SIZE in Enum.java is set to 11000 now
def create_enum_list(maxEnumSize=8, listSize=11000, **kwargs):
    # Allowing length one, we sometimes form single digit numbers that cause the whole column to NA
    # see DparseTask.java for this effect
    # FIX! if we allow 0, then we allow NA?. I guess we check for no missing, so can't allow NA
    # too many retries allowing 1. try 2 min.
    enumList = [random_enum(random.randint(2,maxEnumSize), **kwargs) for i in range(listSize)]
    # a fixed width is sometimes good for finding badness
    ### enumList = [random_enum(4, **kwargs) for i in range(listSize)]
    return enumList

def write_syn_dataset(csvPathname, rowCount, colCount=1, SEED='12345678', 
        colSepChar=",", rowSepChar="\n", quoteChars=""):
    r1 = random.Random(SEED)
    enumList = create_enum_list(quoteChars=quoteChars)

    dsf = open(csvPathname, "w+")
    for row in range(rowCount):
        # doesn't guarantee that 10000 rows have 10000 unique enums in a column
        # essentially sampling with replacement
        rowData = []
        for col in range(colCount):
            ri = random.choice(enumList)
            # first two rows can't tolerate single/double quote randomly
            # keep trying until you get one with no single or double quote in the line
            if row < 2:
                while True:
                    # can't have solely white space cols either in the first two rows
                    if "'" in ri or '"' in ri or h2o_util.might_h2o_think_whitespace(ri):
                        ri = random.choice(enumList)
                    else:
                        break

            rowData.append(ri)

        # output column
        ri = r1.randint(0,1)
        rowData.append(ri)

        # use the new Hive separator
        rowDataCsv = colSepChar.join(map(str,rowData)) + rowSepChar
        ### sys.stdout.write(rowDataCsv)
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

    def test_GLM2_many_enums(self):
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

        ### h2b.browseTheCloud()
        for (rowCount, colCount, hex_key, timeoutSecs) in tryList:
            # just randomly pick the row and col cases.
            colSepCase = random.randint(0,1)
            colSepCase = 1
            # using the comma is nice to ensure no craziness
            if (colSepCase==0):
                colSepHexString = '01'
                quoteChars = ",\'\"" # more choices for the unquoted string
            else:
                colSepHexString = '2c' # comma
                quoteChars = ""

            colSepChar = colSepHexString.decode('hex')
            colSepInt = int(colSepHexString, base=16)
            print "colSepChar:", colSepChar
            print "colSepInt", colSepInt

            rowSepCase = random.randint(0,1)
            # using this instead, makes the file, 'row-readable' in an editor
            if (rowSepCase==0):
                rowSepHexString = '0a' # newline
            else:
                rowSepHexString = '0d0a' # cr + newline (windows) \r\n

            rowSepChar = rowSepHexString.decode('hex')
            print "rowSepChar:", rowSepChar

            SEEDPERFILE = random.randint(0, sys.maxint)
            csvFilename = 'syn_enums_' + str(rowCount) + 'x' + str(colCount) + '.csv'
            csvPathname = SYNDATASETS_DIR + '/' + csvFilename

            print "Creating random", csvPathname
            write_syn_dataset(csvPathname, rowCount, colCount, SEEDPERFILE, 
                colSepChar=colSepChar, rowSepChar=rowSepChar, quoteChars=quoteChars)

            # FIX! does 'separator=' take ints or ?? hex format
            # looks like it takes the hex string (two chars)
            parseResult = h2i.import_parse(path=csvPathname, schema='put', hex_key=hex_key, 
                timeoutSecs=30, separator=colSepInt)
            print "Parse result['destination_key']:", parseResult['destination_key']

            # We should be able to see the parse result?
            ### inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
            print "\n" + csvFilename
            (missingValuesDict, constantValuesDict, enumSizeDict, colTypeDict, colNameDict) = \
                h2o_cmd.columnInfoFromInspect(parseResult['destination_key'], exceptionOnMissingValues=True)

            y = colCount
            kwargs = {'response': y, 'max_iter': 1, 'n_folds': 1, 'alpha': 0.2, 'lambda': 1e-5}
            start = time.time()
            ### glm = h2o_cmd.runGLM(parseResult=parseResult, timeoutSecs=timeoutSecs, pollTimeoutSecs=180, **kwargs)
            print "glm end on ", csvPathname, 'took', time.time() - start, 'seconds'
            ### h2o_glm.simpleCheckGLM(self, glm, None, **kwargs)

            # if not h2o.browse_disable:
            #     h2b.browseJsonHistoryAsUrlLastMatch("Inspect")
            #     time.sleep(5)

if __name__ == '__main__':
    h2o.unit_main()
