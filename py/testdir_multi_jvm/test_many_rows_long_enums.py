import unittest, random, sys, time
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i, h2o_exec as h2e
import codecs, string

# ord('a') gives 97
# str(unichr(97)) gives 'a' 

# MAX_CHAR = 255
MAX_CHAR = 127
ENUM_WIDTH = 64 
JUST_EASY_CHARS = True
JUST_EASIER_CHARS = True

print "Info: If the random chars are all numbers, then quoting them doesn't protect from being seen as number"
print "Need to add inspect/summary checking to the parsed result"
def write_syn_dataset(csvPathname, rowCount, colCount, SEED):
    r1 = random.Random(SEED)
    # dsf = open(csvPathname, "w+")
    # want the full unicode!, not just 0-127 ascii
    dsf = codecs.open(csvPathname,'w+','utf-8')

    for i in range(rowCount):
        rowData = []
        if JUST_EASIER_CHARS:
            # just letters works okay
            # legalChars = string.letters
            # legalChars = "+-.$abcdABCDE01234"
            # legalChars = string.letters + '+-.,$ \t '
            # legalChars = string.letters + '+-.,$ \t '

            # + and - may be considered numbers and flip cols to NA
            # . apparently gets considered as num and can flip cols to NA
            # legalChars = string.letters + "',$ \t +"
            # legalChars = string.printable

            # print "Avoiding plus, minus and period and odd whitespace chars. % also..but $ okay?. Comma causes issues?"

            # print string.punctuation
            # !"#$%&'()*+,-./:;<=>?@[\]^_`{|}~

            otherLegalPunctuation = "!#&()*/:;<=>?@[\]^_`{|}~"
            # legalChars = string.letters + string.digits
            # legalChars = '01234' + "$. "
            # legalChars = '01234' + "+-.,$ \t"
            legalChars = string.letters + string.digits + "+-.,$' \t" + otherLegalPunctuation
        else:
            # same as this
            # legalChars = string.letters + string.punctuation + string.digits + string.whitespace
            legalChars = string.printable

        for j in range(colCount):
            # should be able to handle any character in a quoted stringexcept our 3 eol chars
            # create a random length enum. start with 1024 everywhere
            longEnum = ""

            # do 10% NAs
            n = random.randint(0,9)
            if n==0:
                rowData.append('')
            else:
                for k in range(random.randint(1,ENUM_WIDTH)):
                    if JUST_EASY_CHARS | JUST_EASIER_CHARS:
                        uStr = random.choice(legalChars)
                    else:
                        u = random.randint(0,MAX_CHAR)
                        uStr = str(unichr(u))
                    # we're using double quote below, so don't use that either! double quote is ok!
                    # comma is okay? we're going to force the separator below
                    # TEMP: take out period and plus minus for now
                    if uStr=='\n' or uStr=='\r\n' or uStr=='\r' or uStr=='"' or uStr=='+' or uStr=="-" or uStr==".":
                        # translate to 'a'
                        uStr = 'a'
                    longEnum += uStr
                rowData.append('"' + longEnum + '"') # double quoted long enum

        # can't use ', ' ..illegal, has to be comma only
        rowDataCsv = ",".join(map(str,rowData))
        dsf.write(rowDataCsv + "\n")

    dsf.close()


class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = h2o.setup_random_seed()
        h2o.init(3, java_heap_GB=4)


    @classmethod
    def tearDownClass(cls):
        # time.sleep(3600)
        h2o.tear_down_cloud()

    def test_many_rows_long_enums(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        tryList = [
            (1000000, 1, 'cA', 5),
            (1000000, 1, 'cA', 5),
            ]

        # h2b.browseTheCloud()

        cnum = 0
        for (rowCount, colCount, hex_key, timeoutSecs) in tryList:
            cnum += 1
            csvFilename = 'syn_' + str(SEED) + "_" + str(rowCount) + 'x' + str(colCount) + '.csv'
            csvPathname = SYNDATASETS_DIR + '/' + csvFilename

            print "Creating random", csvPathname
            write_syn_dataset(csvPathname, rowCount, colCount, SEED)

            SEPARATOR = ord(',')
            parseResult = h2i.import_parse(path=csvPathname, schema='put', hex_key=hex_key, timeoutSecs=300, 
                header=0, separator=SEPARATOR) # don't force header..we have NAs in the rows, and NAs mess up headers
            print "Parse result['destination_key']:", parseResult['destination_key']

            # We should be able to see the parse result?
            inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
            missingValuesList = h2o_cmd.infoFromInspect(inspect, csvPathname)
            numCols = inspect['numCols']
            numRows = inspect['numRows']

            print "\n" + csvFilename

if __name__ == '__main__':
    h2o.unit_main()
