import unittest
import random, sys, time, os
import string
import re
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_hosts, h2o_browse as h2b, h2o_import as h2i, h2o_glm

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
        SEED = random.randint(0, sys.maxint)
        # SEED = 
        random.seed(SEED)
        print "\nUsing random seed:", SEED
        global localhost
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(1,java_heap_GB=1)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_GLM_many_enums(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()

        if localhost:
            n = 4000
            tryList = [
                (n, 1000, 'cI', 300), 
                ]
        else:
            n = 5
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
        for (rowCount, colCount, key2, timeoutSecs) in tryList:
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
            parseKey = h2o_cmd.parseFile(None, csvPathname, key2=key2, 
                timeoutSecs=30, separator=colSepInt)
            print csvFilename, 'parse time:', parseKey['response']['time']
            print "Parse result['destination_key']:", parseKey['destination_key']

            # We should be able to see the parse result?
            ### inspect = h2o_cmd.runInspect(None, parseKey['destination_key'])
            print "\n" + csvFilename
            missingValuesDict = h2o_cmd.check_enums_from_inspect(parseKey)
            if missingValuesDict:
                # we allow some NAs in the list above
                pass
                ### m = [str(k) + ":" + str(v) for k,v in missingValuesDict.iteritems()]
                ### raise Exception("Looks like columns got flipped to NAs: " + ", ".join(m))

            y = colCount
            x = range(colCount)
            x = ",".join(map(str,x))
            # kwargs = {'x': x, 'y': y, 'max_iter': 6, 'n_folds': 1, 'alpha': 0.1, 'lambda': 1e-5, 'family': 'poisson', 'case_mode': '=', 'case': 0}
            kwargs = {'y': y, 'max_iter': 6, 'n_folds': 1, 'alpha': 0.1, 'lambda': 1e-5, 'family': 'poisson', 'case_mode': '=', 'case': 0}
            start = time.time()
            glm = h2o_cmd.runGLMOnly(parseKey=parseKey, timeoutSecs=timeoutSecs, pollTimeoutSecs=180, **kwargs)
            print "glm end on ", csvPathname, 'took', time.time() - start, 'seconds'
            h2o_glm.simpleCheckGLM(self, glm, None, **kwargs)

            # if not h2o.browse_disable:
            #     h2b.browseJsonHistoryAsUrlLastMatch("GLM.json")
            #     time.sleep(5)

if __name__ == '__main__':
    h2o.unit_main()
