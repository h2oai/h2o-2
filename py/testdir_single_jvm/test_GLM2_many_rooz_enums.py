import unittest, random, sys, time, re
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i, h2o_glm

targetList = ['red', 'mail', 'black flag', 5, 1981, 'central park', 'good', 'liquor store rooftoop', 'facebook']

lol = [
    ['red','orange','yellow','green','blue','indigo','violet'],
    ['male','female',''],
    ['bad brains','social distortion','the misfits','black flag','iggy and the stooges','the dead kennedys',
        'the sex pistols','the ramones','the clash','green day'],
    range(1,10),
    range(1980,2013),
    ['central park','marin civic center','madison square garden','wembley arena','greenwich village',
     'liquor store rooftop','"woodstock, n.y."','shea stadium'],
    ['good','bad'],
    ['expensive','cheap','free'],
    ['yes','no'],
    ['facebook','twitter','blog',''],
    range(8,100),
    [random.random() for i in range(20)]
]

whitespaceRegex = re.compile(r"""
    ^\s*$     # begin, white space or empty space, end
    """, re.VERBOSE)

DO_TEN_INTEGERS = False
def random_enum(n):
    # pick randomly from a list pointed at by N
    if DO_TEN_INTEGERS:
        # ten choices
        return str(random.randint(0,9))
    else:
        choiceList = lol[n]
        r = str(random.choice(choiceList))
        if r in targetList:
            t = 1
        else:
            t = 0
        return (t,r)

def write_syn_dataset(csvPathname, rowCount, colCount=1, SEED='12345678', 
        colSepChar=",", rowSepChar="\n"):
    r1 = random.Random(SEED)
    dsf = open(csvPathname, "w+")
    for row in range(rowCount):
        # doesn't guarantee that 10000 rows have 10000 unique enums in a column
        # essentially sampling with replacement
        rowData = []
        lenLol = len(lol)
        targetSum = 0
        for col in range(colCount):
            (t,ri) = random_enum(col % lenLol)
            targetSum += t # sum up contributions to output choice
            # print ri
            # first two rows can't tolerate single/double quote randomly
            # keep trying until you get one with no single or double quote in the line
            if row < 2:
                while True:
                    # can't have solely white space cols either in the first two rows
                    if "'" in ri or '"' in ri or whitespaceRegex.match(ri):
                        (t,ri) = random_enum(col % lenLol)
                    else:
                        break

            rowData.append(ri)

        # output column
        avg = (targetSum+0.0)/colCount
        # ri = r1.randint(0,1)
        rowData.append(targetSum)

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
        h2o.tear_down_cloud()

    def test_GLM2_many_rooz_enums(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()

        if 1==0 and localhost:
            n = 4000
            tryList = [
                (n, 999, 'cI', 300), 
                ]
        else:
            n = 100
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
            # can randomly pick the row and col cases.
            ### colSepCase = random.randint(0,1)
            colSepCase = 1
            # using the comma is nice to ensure no craziness
            if (colSepCase==0):
                colSepHexString = '01'
            else:
                colSepHexString = '2c' # comma

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
            if DO_TEN_INTEGERS:
                csvFilename = 'syn_rooz_int10_' + str(rowCount) + 'x' + str(colCount) + '.csv'
            else:
                csvFilename = 'syn_rooz_enums_' + str(rowCount) + 'x' + str(colCount) + '.csv'
            csvPathname = SYNDATASETS_DIR + '/' + csvFilename

            print "Creating random", csvPathname
            write_syn_dataset(csvPathname, rowCount, colCount, SEEDPERFILE, 
                colSepChar=colSepChar, rowSepChar=rowSepChar)

            # FIX! does 'separator=' take ints or ?? hex format
            # looks like it takes the hex string (two chars)
            parseResult = h2i.import_parse(path=csvPathname, schema='put', hex_key=hex_key, 
                timeoutSecs=30, separator=colSepInt)

            # We should be able to see the parse result?
            ### inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
            print "\n" + csvFilename
            # we allow some NAs in the list above
            (missingValuesDict, constantValuesDict, enumSizeDict, colTypeDict, colNameDict) = \
                h2o_cmd.columnInfoFromInspect(parseResult['destination_key'],exceptionOnMissingValues=False)

            y = colCount
            kwargs = {
                'use_all_factor_levels': 1,
                'response': y, 
                'max_iter': 6, 
                'n_folds': 1, 
                'alpha': 0.0, 
                'lambda': 1e-5, 
                'family': 'poisson'
            }
            start = time.time()
            glm = h2o_cmd.runGLM(parseResult=parseResult, timeoutSecs=timeoutSecs, pollTimeoutSecs=180, **kwargs)
            print "glm end on ", csvPathname, 'took', time.time() - start, 'seconds'
            h2o_glm.simpleCheckGLM(self, glm, None, **kwargs)

            # if not h2o.browse_disable:
            #     h2b.browseJsonHistoryAsUrlLastMatch("GLM.json")
            #     time.sleep(5)

if __name__ == '__main__':
    h2o.unit_main()
