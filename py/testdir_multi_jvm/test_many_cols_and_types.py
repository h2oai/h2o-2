import unittest, random, sys, time, os
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i, h2o_exec as h2e

# do a random shuffle of 0 thru COLCASES-1, to say what case a column uses
COLCASES = 12

def write_syn_dataset(csvPathname, rowCount, colCount, SEEDPERFILE):
    letters = 'abcdefghijklmnopqrstuvwxyz'
    # all zeroes
    def case0(r):
        return '0'
    # all ones
    def case1(r):
        return '1'
    # rand 0 or 1
    def case2(r):
        return str(r.randint(0,1))
    # rand 0-2, 2 is NA
    # UPDATE: remove NA case..so the info check doesn't see cols flipped to NA
    def case3(r):
        d = r.randint(0,2)
        return str(d)
    # all 'a'
    def case4(r):
        return 'a'
    # random single char (1 of 24 enum)
    def case5(r):
        return str(random.choice(letters))
    # random 1 or 2 chars
    def case6(r):
        d = r.randint(0,1)
        if d==0:
            return random.choice(letters)
        else:
            return random.choice(letters) + random.choice(letters)
    # random real no leading 0, 1 digit
    def case7(r):
        d = r.random()
        return ("%0.1f" % d)
    # random real no leading 0, 2 digit
    def case8(r):
        d = r.random()
        return ("%0.2f" % d)
    # random real no leading 0, 1 or 2 digit
    def case9(r):
        d = r.random()
        d1 = r.randint(0,1)
        if (d1==0):
            return ("%0.1f" % d)
        else:
            return ("%0.2f" % d)
    # random real 1 leading digit , 1 or 2 digit
    def case10(r):
        d = r.random()
        d1 = r.randint(0,1)
        if (d1==0):
            return ("%1.1f" % d)
        else:
            return ("%1.2f" % d)
    # random real full precision
    def case11(r):
        return str(r.uniform(-1e30,1e30))

    # try a neat way to use a dictionary to case select functions
    caseList=[case0,case1,case2,case3,case4,case5,case6,case7,case8,case9,case10,case11]
    r = random.Random(SEEDPERFILE)
    # this makes sure we hit all cases for small col counts
    # I guess we can just use this mod COLCASES if col count is bigger
    colCase = range(COLCASES)
    random.shuffle(colCase)

    dsf = open(csvPathname, "w+")
    for i in range(rowCount):
        rowData = []
        for j in range(colCount):
            colCaseToUse = j % COLCASES
            f = caseList[colCase[colCaseToUse]]
            rowData.append(f(r)) # f should always return string
        rowDataCsv = ",".join(rowData)
        dsf.write(rowDataCsv + "\n")
    dsf.close()

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = h2o.setup_random_seed()
        h2o.init(2,java_heap_GB=1)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_many_cols_and_types(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        tryList = [
            (100, 5, 'cA', 5),
            (1000, 59, 'cB', 5),
            (5000, 128, 'cC', 5),
            (6000, 507, 'cD', 5),
            (9000, 663, 'cE', 5),
            ]
        
        for (rowCount, colCount, hex_key, timeoutSecs) in tryList:
            SEEDPERFILE = random.randint(0, sys.maxint)
            csvFilename = "syn_%s_%s_%s.csv" % (SEEDPERFILE, rowCount, colCount)
            csvPathname = SYNDATASETS_DIR + '/' + csvFilename

            print "Creating random", csvPathname
            write_syn_dataset(csvPathname, rowCount, colCount, SEEDPERFILE)

            parseResult = h2i.import_parse(path=csvPathname, schema='put', hex_key=hex_key, timeoutSecs=30)
            print "Parse result['destination_key']:", parseResult['destination_key']
            inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
            h2o_cmd.infoFromInspect(inspect, csvPathname)

            print "\n" + csvFilename

if __name__ == '__main__':
    h2o.unit_main()
