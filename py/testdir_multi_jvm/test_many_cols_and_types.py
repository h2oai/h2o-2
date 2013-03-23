import unittest
import random, sys, time, os
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_hosts, h2o_browse as h2b, h2o_import as h2i, h2o_exec as h2e

def write_syn_dataset(csvPathname, rowCount, colCount, SEEDPERFILE, sel):

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
    def case3(r):
        d = r.randint(0,2)
        if (d==2):
            return ''
        else:
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

    # try a neat way to use a dictitionary to case select functions
    caseList=[case0,case1,case2,case3,case4,case5,case6,case7,case8,case9,case10,case11]

    if sel<0 or sel>=len(caseList):
        raise Exception("sel out of range in write_syn_dataset:", sel)
    f = caseList[sel]

    # we can do all sorts of methods off the r object
    r = random.Random(SEEDPERFILE)

    dsf = open(csvPathname, "w+")
    for i in range(rowCount):
        rowData = []
        for j in range(colCount):
            rowData.append(f(r)) # f should always return string
        rowDataCsv = ",".join(rowData)
        dsf.write(rowDataCsv + "\n")
    dsf.close()

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(2,java_heap_GB=1)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_many_cols_and_values_with_syn(self):
        SEED = random.randint(0, sys.maxint)
        print "\nUsing random seed:", SEED
        # SEED =
        random.seed(SEED)
        SYNDATASETS_DIR = h2o.make_syn_dir()
        tryList = [
            (100, 10000, 'cA', 5),
            (100, 1000, 'cB', 5),
            (100, 900, 'cC', 5),
            (100, 500, 'cD', 5),
            (100, 100, 'cE', 5),
            ]
        
        for (rowCount, colCount, key2, timeoutSecs) in tryList:
            for sel in range(12):
                SEEDPERFILE = random.randint(0, sys.maxint)
                csvFilename = "syn_%s_%s_%s_%s.csv" % (SEEDPERFILE, sel, rowCount, colCount)
                csvPathname = SYNDATASETS_DIR + '/' + csvFilename

                print "Creating random", csvPathname
                write_syn_dataset(csvPathname, rowCount, colCount, SEEDPERFILE, sel)

                selKey2 = key2 + "_" + str(sel)
                parseKey = h2o_cmd.parseFile(None, csvPathname, key2=selKey2, timeoutSecs=30)
                print csvFilename, 'parse time:', parseKey['response']['time']
                print "Parse result['destination_key']:", parseKey['destination_key']
                inspect = h2o_cmd.runInspect(None, parseKey['destination_key'])
                print "\n" + csvFilename

                # if not h2o.browse_disable:
                    # h2b.browseJsonHistoryAsUrlLastMatch("Inspect")
                    # time.sleep(5)

if __name__ == '__main__':
    h2o.unit_main()
