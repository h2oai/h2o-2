import unittest, random, sys, time, re
sys.path.extend(['.','..','../..','py'])

import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i, h2o_glm, h2o_util
RANDOM_LENGTH = False
ENUM_SIZE = random.randint(2,7)
### ENUM_SIZE = 4
# just randomly pick the row and col cases.
COL_SEP_HIVE = random.randint(0,1) == 1
COL_SEP_HIVE = False # comma

# details:
# Apparently we don't have any new EOL separators for hive?, just new column separator
# We allow extra chars in the hive separated columns..i.e. single and double quote.

# we want to seed a random dictionary for our enums
# python has some things we can use
# string.ascii_uppercase string.printable string.letters string.digits string.punctuation string.whitespace
# restricting the choices makes it easier to find the bad cases

DO_SIMPLE_CHARS_ONLY = True
def random_enum(enumSize, randChars="abeE01" + "" if DO_SIMPLE_CHARS_ONLY else "$%+-.;|\t ", quoteChars="\'\""):
    choiceStr = randChars + quoteChars
    mightBeNumberOrWhite = True
    while mightBeNumberOrWhite:
        # H2O doesn't seem to tolerate random single or double quote in the first two rows.
        # disallow that by not passing quoteChars for the first two rows (in call to here)
        r = ''.join(random.choice(choiceStr) for x in range(enumSize))
        mightBeNumberOrWhite = h2o_util.might_h2o_think_number_or_whitespace(r)
    return r

def write_syn_dataset(csvPathname, rowCount, colCount=1, SEED='12345678', 
        colSepChar=",", rowSepChar="\n", quoteChars=""):

    r1 = random.Random(SEED)
    dsf = open(csvPathname, "w+")
    for row in range(rowCount):
        # doesn't guarantee that 10000 rows have 10000 unique enums in a column
        # essentially sampling with replacement
        rowData = []
        def doRandomOrFixed():
            if RANDOM_LENGTH:
                return random_enum(random.randint(2, ENUM_SIZE), quoteChars=quoteChars)
            else:
                return random_enum(ENUM_SIZE, quoteChars=quoteChars)

        for col in range(colCount):
            ri = doRandomOrFixed()
            # first two rows can't tolerate single/double quote randomly
            # keep trying until you get one with no single or double quote in the line
            if row < 2:
                while True:
                    # can't have solely white space cols either in the first two rows
                    if "'" in ri or '"' in ri or h2o_util.might_h2o_think_whitespace(ri):
                        ri = doRandomOrFixed()
                    else:
                        break
            rowData.append(ri)

        # output column
        ri = r1.randint(0,1)
        rowData.append(ri)

        # use the new Hive separator
        rowDataCsv = colSepChar.join(map(str,rowData)) + rowSepChar
        sys.stdout.write(rowDataCsv)
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

    def test_find_numbers(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()

        n = 7 
        tryList = [
            (n, 1, 'cD', 300), 
            (n, 2, 'cE', 300), 
            (n, 3, 'cF', 300), 
            (n, 4, 'cG', 300), 
            (n, 5, 'cH', 300), 
            (n, 6, 'cI', 300), 
            (n, 7, 'cJ', 300), 
            (n, 9, 'cK', 300), 
            (n, 10, 'cLA', 300), 
            (n, 11, 'cDA', 300), 
            (n, 12, 'cEA', 300), 
            (n, 13, 'cFA', 300), 
            (n, 14, 'cGA', 300), 
            (n, 15, 'cHA', 300), 
            (n, 16, 'cIA', 300), 
            (n, 17, 'cJA', 300), 
            (n, 19, 'cKA', 300), 
            (n, 20, 'cLA', 300), 
            ]

        ### h2b.browseTheCloud()
        for (rowCount, colCount, hex_key, timeoutSecs) in tryList:
            # using the comma is nice to ensure no craziness
            if COL_SEP_HIVE:
                colSepHexString = '01'
                singleQuotes = 1 # allow single quotes to be delimiter
                quoteChars = ",\'\"" # more choices for the unquoted string
            else:
                colSepHexString = '2c' # comma
                singleQuotes = 0 #  single quotes are not delimiters
                quoteChars = "'"

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
            parseResult = h2i.import_parse(path=csvPathname, schema='put', hex_key=hex_key, single_quotes=singleQuotes,
                timeoutSecs=30, separator=colSepInt)
            print "Parse result['destination_key']:", parseResult['destination_key']

            # We should be able to see the parse result?
            ### inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
            print "\n" + csvFilename
            (missingValuesDict, constantValuesDict, enumSizeDict, colTypeDict, colNameDict) = \
                h2o_cmd.columnInfoFromInspect(parseResult['destination_key'], exceptionOnMissingValues=True)

if __name__ == '__main__':
    h2o.unit_main()
