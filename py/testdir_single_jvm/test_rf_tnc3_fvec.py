import unittest, time, sys
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_import as h2i
import h2o_browse as h2b
import h2o_exec as h2e

print "colswap doesn't seem like it can update a col with NAs correctly."
print "smalldata/tnc3.csv was modified all missing entries replaced with <dquote>-1<dquote>." 
print "then all starting quotes are replaced with <dquote><space>"
print "so quoted numbers get treated as strings by parser"
print "But that leaves NAs in the number cols?"
numExprList = [
        # 'tnc3.hex = colSwap(tnc3.hex,<col1>,tnc3.hex[<col1>] + 0)',
        # 'tnc3.hex = colSwap(tnc3.hex,<col1>,(tnc3.hex[<col1>]==0 ? 9999 : tnc3.hex[<col1>]))',
        # 'tnc3.hex = colSwap( tnc3.hex, <col1>, (tnc3.hex[<col1>] + 1) )',
        'apply(tnc3.hex,1,function(x){ifelse(x==-1,1,x)})'
    ]

charExprList = [
        'apply(tnc3.hex,1,function(x){ifelse(x==-1,"na",x)})'
    ]

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.init(1)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_rf_tnc3_fvec(self):
        csvPathname = 'tnc3.csv'
        print "\n" + csvPathname
        hex_key = "tnc3.hex"
        ### h2b.browseTheCloud()

        parseResult = h2i.import_parse(bucket='smalldata', path=csvPathname, hex_key=hex_key, schema='put', 
            timeoutSecs=10, header=1)
        print "Parse result['Key']:", parseResult['destination_key']
        inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
        h2b.browseJsonHistoryAsUrlLastMatch("Inspect")

        if 1==1:
            lenNodes = len(h2o.nodes)
            colResultList = h2e.exec_expr_list_across_cols(lenNodes, numExprList, hex_key, maxCol=10,
                incrementingResult=False, timeoutSecs=10)
            print "\ncolResultList after num swap", colResultList

        if (1==1):
            print "\nWe're not CM data getting back from RFView.json that we can check!. so look at the browser"
            print 'The good case with ignore="boat,body"'
            rfv = h2o_cmd.runRF(parseResult=parseResult, trees=5, timeoutSecs=10, ignored_cols_by_name="boat,body")

        inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
        ### h2b.browseJsonHistoryAsUrlLastMatch("Inspect")
        ### time.sleep(3600)
        h2b.browseJsonHistoryAsUrlLastMatch("RFView")

        #******************
        if 1==0:
            colResultList = h2e.exec_expr_list_across_cols(lenNodes, charExprList, hex_key, maxCol=10,
                incrementingResult=False, timeoutSecs=10)
            print "\ncolResultList after char swap", colResultList

        if 1==1:
            print "\nNow the bad case (no ignore)"
            rfv = h2o_cmd.runRF(parseResult=parseResult, trees=5, timeoutSecs=10)

        inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
        ### h2b.browseJsonHistoryAsUrlLastMatch("Inspect")
        ### time.sleep(3600)
        h2b.browseJsonHistoryAsUrlLastMatch("RFView")


if __name__ == '__main__':
    h2o.unit_main()
