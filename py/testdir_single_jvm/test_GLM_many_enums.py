import unittest
import random, sys, time, os
import string
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_hosts, h2o_browse as h2b, h2o_import as h2i, h2o_glm

# we want to seed a random dictionary for our enums
# string.ascii_uppercase
# string.printable):
# string.letters + string.digits + string.punctuation + string.whitespace):
# can use comma when hive 01 is used
# FIX! remove \' and \" temporarily

# Apparently we don't have any new EOL separators for hive?
# def random_enum(maxEnumSize=6, randChars=string.letters + string.digits + ",;|\t "):
def random_enum(maxEnumSize=8, randChars=string.letters + "012"):
# def random_enum(maxEnumSize=6, randChars=string.letters + string.digits):
    return ''.join(random.choice(randChars) for x in range(maxEnumSize))


# MAX_ENUM_SIZE in Enum.java is set to 11000 now
def create_enum_list(maxEnumSize=8, listSize=11000, **kwargs):
    # allowing length one, we sometimes form single digit numbers that cause the whole column to NA
    # see DparseTask.java for this effect
    # Use min 4..unlikely to get a random that looks like number with 4 and our string above?j
    
    enumList = [random_enum(random.randint(4,maxEnumSize), **kwargs) for i in range(listSize)]
    return enumList

def write_syn_dataset(csvPathname, rowCount, colCount=1, SEED='12345678', 
    colSepChar=",", rowSepChar="\n"):
    r1 = random.Random(SEED)
    enumList = create_enum_list()

    dsf = open(csvPathname, "w+")
    for i in range(rowCount):
        # doesn't guarantee that 10000 rows have 10000 unique enums in a column
        # essentially sampling with replacement
        rowData = []
        for j in range(colCount):
            ri = random.choice(enumList)
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
        SEED = random.randint(0, sys.maxint)
        # SEED = 
        random.seed(SEED)
        print "\nUsing random seed:", SEED
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(1,java_heap_GB=10)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        ### time.sleep(3600)
        h2o.tear_down_cloud()

    def test_GLM_many_cols(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        tryList = [
            (2000, 1, 'cD', 300), 
            (2000, 2, 'cE', 300), 
            (2000, 3, 'cF', 300), 
            (2000, 4, 'cG', 300), 
            (2000, 5, 'cH', 300), 
            (2000, 6, 'cI', 300), 
            (2000, 7, 'cJ', 300), 
            (2000, 8, 'cK', 300), 
            ]

        ### h2b.browseTheCloud()
        lenNodes = len(h2o.nodes)
        # using the comma is nice to ensure no craziness
        colSepHexString = '01'
        colSepHexString = '2c' # comma
        colSepChar = colSepHexString.decode('hex')
        colSepInt = int(colSepHexString, base=16)
        print "colSepChar:", colSepChar
        print "colSepInt", colSepInt

        # using this instead, makes the file, 'row-readable' in an editor
        rowSepHexString = '11'
        rowSepHexString = '0a' # newline
        rowSepHexString = '0d0a' # cr + newline (windows) \r\n
        rowSepChar = rowSepHexString.decode('hex')
        print "rowSepChar:", rowSepChar

        for (rowCount, colCount, key2, timeoutSecs) in tryList:
            SEEDPERFILE = random.randint(0, sys.maxint)
            csvFilename = 'syn_' + str(SEEDPERFILE) + "_" + str(rowCount) + 'x' + str(colCount) + '.csv'
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
            h2o_cmd.check_enums_from_inspect(parseKey)

            y = colCount
            kwargs = {'y': y, 'max_iter': 1, 'n_folds': 1, 'alpha': 0.2, 'lambda': 1e-5, 
                'case_mode': '=', 'case': 0}
            start = time.time()
            glm = h2o_cmd.runGLMOnly(parseKey=parseKey, timeoutSecs=timeoutSecs, pollTimeoutSecs=180, **kwargs)
            print "glm end on ", csvPathname, 'took', time.time() - start, 'seconds'
            h2o_glm.simpleCheckGLM(self, glm, None, **kwargs)

            # if not h2o.browse_disable:
            #     h2b.browseJsonHistoryAsUrlLastMatch("Inspect")
            #     time.sleep(5)


if __name__ == '__main__':
    h2o.unit_main()
