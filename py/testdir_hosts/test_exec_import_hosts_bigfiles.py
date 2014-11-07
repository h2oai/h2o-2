import unittest
import random, sys, time
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i, h2o_exec as h2e

zeroList = [
        'Result.hex = c(0)',
        'Result0.hex = c(0)',
        'Result1.hex = c(0)',
        'Result2.hex = c(0)',
        'Result3.hex = c(0)',
        'Result4.hex = c(0)',
]

exprList = [
        'Result<n>.hex = log(<keyX>[,<col1>])',
        'Result<n>.hex = factor(<keyX>[,<col1>])',
        'Result<n>.hex = <keyX>[,<col1>]',
        # this makes a scalar if not added to a column?
        'Result<n>.hex = min(<keyX>[,<col1>]) + Result<n-1>.hex',
        'Result<n>.hex = max(<keyX>[,<col1>]) + Result<n-1>.hex',
        'Result<n>.hex = mean(<keyX>[,<col1>]) + Result<n-1>.hex',
        'Result<n>.hex = sum(<keyX>[,<col1>]) + Result.hex',
    ]

def exec_list(exprList, lenNodes, csvFilename, hex_key, colX):
        h2e.exec_zero_list(zeroList)
        # start with trial = 1 because trial-1 is used to point to Result0 which must be initted
        trial = 1
        while (trial < 100):
            for exprTemplate in exprList:
                # do each expression at a random node, to facilate key movement
                nodeX = random.randint(0,lenNodes-1)
                # billion rows only has two cols
                # colX is incremented in the fill_in_expr_template

                # FIX! should tune this for covtype20x vs 200x vs covtype.data..but for now
                row = str(random.randint(1,400000))

                execExpr = h2e.fill_in_expr_template(exprTemplate, colX, trial, row, hex_key)
                execResultInspect = h2e.exec_expr(h2o.nodes[nodeX], execExpr, 
                    resultKey="Result"+str(trial)+".hex", timeoutSecs=60)

                h2o.check_sandbox_for_errors()
                print "Trial #", trial, "completed\n"
                trial += 1

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = h2o.setup_random_seed()
        h2o.init()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_exec_import_hosts_bigfiles(self):
        # just do the import folder once
        timeoutSecs = 4000

        #    "covtype169x.data",
        #    "covtype.13x.shuffle.data",
        #    "3G_poker_shuffle"
        # Update: need unique key names apparently. can't overwrite prior parse output key?
        # replicating lines means they'll get reparsed. good! (but give new key names)

        csvFilenameList = [
            ("covtype.data", "c"),
            ("covtype20x.data", "c20"),
            ("covtype200x.data", "c200"),
            # can't do enum 
            # ("billion_rows.csv.gz", "b"),
            ]

        # h2b.browseTheCloud()
        lenNodes = len(h2o.nodes)
        importFolderPath = "standard"
        for (csvFilename, hex_key) in csvFilenameList:
            csvPathname = importFolderPath + "/" + csvFilename
            parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathname, schema='local', hex_key=hex_key, 
                retryDelaySecs=3, timeoutSecs=2000)
            print "Parse result['destination_key']:", parseResult['destination_key']
            inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
            print "\n" + csvFilename

            # last column
            colX = inspect['numCols'] - 1
            exec_list(exprList, lenNodes, csvFilename, hex_key, colX)


if __name__ == '__main__':
    h2o.unit_main()
