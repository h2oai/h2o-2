import unittest, random, sys, time
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_browse as h2b, h2o_exec as h2e, h2o_import as h2i, h2o_cmd
import h2o_print as h2p, h2o_summ

# quantile lists of 10, 100)
exprList = []

QUANTILE = 0.25
print "stress the 1000 fixed binning based on (max-min)/1000"
a = [
    -1.0000002e10,
    -1.0000001e10,
    -1.0000000e10,
    -1.0000002e9,
    -1.0000001e9,
    -1.0000000e9,
    -1.0000002e6,
    -1.0000001e6,
    -1.0000000e6,
    -1.0000002e3,
    -1.0000001e3,
    -1.0000000e3,
    -1.0,
     0.0000000,
     1.0,
    1.0000002e3,
    1.0000001e3,
    1.0000000e3,
    1.0000002e6,
    1.0000001e6,
    1.0000000e6,
    1.0000002e9,
    1.0000001e9,
    1.0000000e9,
    1.0000002e10,
    1.0000001e10,
    1.0000000e10
]

initList = [
    "ddd = c(%s)" % ",".join(map(str,a))
]

# get expected result
a.sort()
expectedP = h2o_summ.percentileOnSortedList(a, QUANTILE, interpolate='linear')
print "expectedP:", expectedP
h2p.blue_print("sort result, expectedP:", expectedP)

exprList = [
    ("abc = quantile(ddd[,1], c(%s))" % QUANTILE, 1),
]

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = h2o.setup_random_seed()
        h2o.init(1, java_heap_GB=1)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_exec2_quantile_na_scalar(self):
        for execExpr in initList:
            h2e.exec_expr(h2o.nodes[0], execExpr, resultKey=None, timeoutSecs=180)

        for (execExpr, num) in exprList:
            start = time.time()
            resultExec, result = h2e.exec_expr(h2o.nodes[0], execExpr, resultKey=None, timeoutSecs=180)
            print 'exec end took', time.time() - start, 'seconds'
            h2p.blue_print("h2o exec quantiles result:", result)
            self.assertEqual(result, expectedP, msg="Checking exec quantiles median, expectedP: %s result: %s" % (expectedP, result))
            print h2o.dump_json(resultExec)
            # do the quantiles page on the created key
            kwargs = {
                'column': 0,
                'quantile': QUANTILE,
                'multiple_pass': 2,
                'max_qbins': 1000,
            }
            q = h2o.nodes[0].quantiles(source_key='ddd', **kwargs)
            qresult = q['result']
            qresult_single = q['result_single']
            qresult_iterations = q['iterations']
            qresult_interpolated = q['interpolated']
            h2p.blue_print("h2o quantiles result:", qresult)
            h2p.blue_print("h2o quantiles result_single:", qresult_single)
            h2p.blue_print("h2o quantiles iterations:", qresult_iterations)
            h2p.blue_print("h2o quantiles interpolated:", qresult_interpolated)
            print h2o.dump_json(q)

            self.assertEqual(qresult_iterations, 3, msg="should take 3 iterations")

            # self.assertEqual(qresult_interpolated, True, msg="Should say it's interpolating")
            
            self.assertEqual(qresult, expectedP, msg="Checking quantilespage median, expectedP: %s result: %s" % (expectedP, qresult))

            inspect = h2o_cmd.runInspect(key='abc')
            numCols = inspect['numCols']
            numRows = inspect['numRows']
            print "numCols:", numCols
            print "numRows:", numRows
            self.assertEqual(numCols, 1)
            self.assertEqual(numRows, num)

            h2o.check_sandbox_for_errors()


if __name__ == '__main__':
    h2o.unit_main()
