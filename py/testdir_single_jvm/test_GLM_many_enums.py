import unittest
import random, sys, time, os
import string
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_hosts, h2o_browse as h2b, h2o_import as h2i, h2o_glm

# we want to seed a random dictionary for our enums
# this creates random stringsj
# id_generator()
# id_generator(3, "6793YUIO")
# id_generator(string.ascii_uppercase, "6793YUIO")
# def random_enum(size=6, chars=string.printable):
# def random_enum(size=6, chars=string.letters + string.digits + string.punctuation + string.whitespace):
def random_enum(size=6, chars=string.letters + string.digits):
    return ''.join(random.choice(chars) for x in range(size))

def create_enum_list(size=1000):
    enumList = [random_enum() for i in range(size)]
    return enumList

def write_syn_dataset(csvPathname, rowCount, colCount=1, SEED='12345678', 
    colSepChar=",", rowSepChar="\n"):
    r1 = random.Random(SEED)
    enumList = create_enum_list()

    dsf = open(csvPathname, "w+")
    for i in range(rowCount):
        rowData = []
        for j in range(colCount):
            ri = random.choice(enumList)
            rowData.append(ri)

        # output column
        ri = r1.randint(0,1)
        rowData.append(ri)

        # use the new Hive separator
        rowDataCsv = colSepChar.join(map(str,rowData)) + rowSepChar
        print rowDataCsv
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
            (10000, 1, 'cD', 300), 
            (10000, 2, 'cE', 300), 
            (10000, 3, 'cF', 300), 
            (10000, 4, 'cG', 300), 
            (10000, 5, 'cH', 300), 
            (10000, 1, 'cI', 300), 
            ]

        ### h2b.browseTheCloud()
        lenNodes = len(h2o.nodes)
        colSepHexString = '01'
        colSepHexString = '0a'
        colSepChar = colSepHexString.decode('hex')
        colSepInt = int(colSepHexString, base=16)

        print "colSepChar:", colSepChar
        print "colSepInt", colSepInt

        rowSepHexString = '01'
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
            inspect = h2o_cmd.runInspect(None, parseKey['destination_key'])
            print "\n" + csvFilename

            y = colCount
            kwargs = {'y': y, 'max_iter': 50, 'n_folds': 3, 'alpha': 0.2, 'lambda': 1e-5}
            start = time.time()
            glm = h2o_cmd.runGLMOnly(parseKey=parseKey, timeoutSecs=timeoutSecs, **kwargs)
            print "glm end on ", csvPathname, 'took', time.time() - start, 'seconds'
            h2o_glm.simpleCheckGLM(self, glm, None, **kwargs)

            # if not h2o.browse_disable:
            #     h2b.browseJsonHistoryAsUrlLastMatch("Inspect")
            #     time.sleep(5)


if __name__ == '__main__':
    h2o.unit_main()
