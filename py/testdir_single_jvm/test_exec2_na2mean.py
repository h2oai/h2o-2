import unittest, random, sys, time
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_browse as h2b, h2o_exec as h2e, h2o_import as h2i, h2o_cmd, h2o_util

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = h2o.setup_random_seed()
        h2o.init(1)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_exec2_na2mean(self):
        print "https://0xdata.atlassian.net/browse/PUB-228"
        bucket = 'home-0xdiag-datasets'
        csvPathname = 'standard/covtype.data'
        hexKey = 'r.hex'
        parseResult = h2i.import_parse(bucket=bucket, path=csvPathname, schema='put', hex_key=hexKey)
        # work up to the failing case incrementally
        execExprList = [
            # hack to make them keys? (not really needed but interesting)
            'rcnt = c(0)',
            'total = c(0)',
            'mean = c(0)',
            's.hex = r.hex',
            "x=r.hex[,1]; rcnt=nrow(x)-sum(is.na(x))",
            "x=r.hex[,1]; total=sum(ifelse(is.na(x),0,x)); rcnt=nrow(x)-sum(is.na(x))",
            "x=r.hex[,1]; total=sum(ifelse(is.na(x),0,x)); rcnt=nrow(x)-sum(is.na(x)); mean=total / rcnt",
            "x=r.hex[,1]; total=sum(ifelse(is.na(x),0,x)); rcnt=nrow(x)-sum(is.na(x)); mean=total / rcnt; x=ifelse(is.na(x),mean,x)",
        ]

        execExprList2 = [
            "s.hex = apply(r.hex,2," +
                "function(x){total=sum(ifelse(is.na(x),0,x)); " + \
                "rcnt=nrow(x)-sum(is.na(x)); " + \
                "mean=total / rcnt; " + \
                "ifelse(is.na(x),mean,x)} " + \
            ")" ,
            # this got an exception. note I forgot to assign to x here
            "s=r.hex[,1]; s.hex[,1]=ifelse(is.na(x),0,x)",
            # throw in a na flush to 0
            "x=r.hex[,1]; s.hex[,1]=ifelse(is.na(x),0,x)",
        ]
        execExprList += execExprList2

        results = []
        for execExpr in execExprList:
            start = time.time()
            (resultExec, result) = h2e.exec_expr(execExpr=execExpr, timeoutSecs=30) # unneeded but interesting 
            results.append(result)
            print "exec end on ", "operators" , 'took', time.time() - start, 'seconds'
            print "exec result:", result
            print "exec result (full):", h2o.dump_json(resultExec)
            h2o.check_sandbox_for_errors()

        # compare it to summary
        rSummary = h2o_cmd.runSummary(key='r.hex', cols='0')
        h2o_cmd.infoFromSummary(rSummary)

        sSummary = h2o_cmd.runSummary(key='s.hex', cols='0')
        h2o_cmd.infoFromSummary(sSummary)

        # since there are no NAs in covtype, r.hex and s.hex should be identical?
        print "Comparing summary of r.hex to summary of s.hex"
        df = h2o_util.JsonDiff(rSummary, sSummary, with_values=True)
        # time can be different
        print "df.difference:", h2o.dump_json(df.difference)
        self.assertLess(len(df.difference), 2)
    

        print "results from the individual exec expresssions (ignore last which was an apply)"
        print "results:", results
        self.assertEqual(results, [0.0, 0.0, 0.0, 1859.0, 581012.0, 581012.0, 2959.365300544567, 1859.0, 1859.0, 1859.0, 1859.0])



if __name__ == '__main__':
    h2o.unit_main()
