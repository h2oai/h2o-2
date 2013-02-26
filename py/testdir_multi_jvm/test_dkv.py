import unittest
import random, sys, time, os
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_hosts, h2o_browse as h2b, h2o_import as h2i, h2o_exec as h2e

def write_syn_dataset(csvPathname, rowCount, SEED):
    # 8 random generatators, 1 per column
    r1 = random.Random(SEED)
    r2 = random.Random(SEED)
    r3 = random.Random(SEED)
    r4 = random.Random(SEED)
    r5 = random.Random(SEED)
    r6 = random.Random(SEED)
    r7 = random.Random(SEED)
    r8 = random.Random(SEED)
    dsf = open(csvPathname, "w+")
    for i in range(rowCount):
        rowData = "%s,%s,%s,%s,%s,%s,%s,%s" % (
            r1.randint(0,1),
            r2.randint(0,2),
            r3.randint(-4,4),
            r4.randint(0,8),
            r5.randint(-16,16),
            r6.randint(-32,32),
            0,
            r8.randint(0,1))
        dsf.write(rowData + "\n")
    dsf.close()

zeroList = [
        'Result0 = 0',
        'Result.hex = 0',
]

exprList = [
        'Result<n> = max(<keyX>[<col1>])',
    ]

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        SEED = random.randint(0, sys.maxint)
        # if you have to force to redo a test
        # SEED = 
        random.seed(SEED)
        print "\nUsing random seed:", SEED
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(2,java_heap_GB=1)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        # wait while I inspect things
        # time.sleep(1500)
        h2o.tear_down_cloud()

    def test_enum_with_syn(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        csvFilenameAll = [
            ("syn_10x8.csv", 'cA', 5),
            ]

        ### csvFilenameList = random.sample(csvFilenameAll,1)
        csvFilenameList = csvFilenameAll
        ### h2b.browseTheCloud()
        lenNodes = len(h2o.nodes)

        for (csvFilename, key2, timeoutSecs) in csvFilenameList:
            SEEDPERFILE = random.randint(0, sys.maxint)
            csvPathname = SYNDATASETS_DIR + '/' + csvFilename
            print "Creating random 10x8 csv"
            write_syn_dataset(csvPathname, 10, SEEDPERFILE)
            # creates csvFilename.hex from file in importFolder dir 
            parseKey = h2o_cmd.parseFile(None, csvPathname, key2=key2, timeoutSecs=2000)
            print csvFilename, 'parse time:', parseKey['response']['time']
            print "Parse result['destination_key']:", parseKey['destination_key']

            # We should be able to see the parse result?
            inspect = h2o_cmd.runInspect(None, parseKey['destination_key'])

            print "\n" + csvFilename
            h2e.exec_zero_list(zeroList)
            # does n+1 so use maxCol 6
            h2e.exec_expr_list_rand(lenNodes, exprList, key2, 
                maxCol=0, maxRow=400000, maxTrials=100, timeoutSecs=timeoutSecs)


if __name__ == '__main__':
    h2o.unit_main()
