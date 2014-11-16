import unittest, random, sys, time
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i, h2o_util, h2o_exec

zeroList = [
        'Result0 = 0',
]

# the first column should use this
exprList = [
        'Result<n> = sum(<keyX>[,<col1>])',
    ]

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = h2o.setup_random_seed()
        h2o.init(2)

        global SYNDATASETS_DIR
        SYNDATASETS_DIR = h2o.make_syn_dir()

    @classmethod
    def tearDownClass(cls):
        # wait while I inspect things
        # time.sleep(1500)
        h2o.tear_down_cloud()

    def test_exec2_sum(self):
        print "Replicating covtype.data by 2x for results comparison to 1x"
        filename1x = 'covtype.data'
        pathname1x = h2i.find_folder_and_filename('home-0xdiag-datasets', 'standard/covtype.data', returnFullPath=True)
        filename2x = "covtype_2x.data"
        pathname2x = SYNDATASETS_DIR + '/' + filename2x
        h2o_util.file_cat(pathname1x, pathname1x, pathname2x)

        csvAll = [
            (pathname1x, "cA", 5,  1),
            (pathname2x, "cB", 5,  2),
            (pathname2x, "cC", 5,  2),
        ]

        h2b.browseTheCloud()
        lenNodes = len(h2o.nodes)

        firstDone = False
        for (csvPathname, hex_key, timeoutSecs, resultMult) in csvAll:
            parseResult = h2i.import_parse(path=csvPathname, schema='put', hex_key=hex_key, timeoutSecs=2000)
            print "Parse result['Key']:", parseResult['destination_key']

            # We should be able to see the parse result?
            inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])

            print "\n" + csvPathname
            h2o_exec.exec_zero_list(zeroList)
            colResultList = h2o_exec.exec_expr_list_across_cols(lenNodes, exprList, hex_key, maxCol=54, 
                timeoutSecs=timeoutSecs)
            print "\ncolResultList", colResultList

            if not firstDone:
                colResultList0 = list(colResultList)
                good = [float(x) for x in colResultList0] 
                firstDone = True
            else:
                print "\n", colResultList0, "\n", colResultList
                # create the expected answer...i.e. N * first
                compare = [float(x)/resultMult for x in colResultList] 
                print "\n", good, "\n", compare
                self.assertEqual(good, compare, 'compare is not equal to good (first try * resultMult)')

if __name__ == '__main__':
    h2o.unit_main()
