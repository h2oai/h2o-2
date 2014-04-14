import unittest, random, sys, time
sys.path.extend(['.','..','py'])
import h2o, h2o_browse as h2b, h2o_exec as h2e, h2o_hosts, h2o_import as h2i

initList = [
    ('r.hex', 'r.hex=i.hex'),
]

DO_COMPOUND = False

phrasesCompound = [
    "a=1; a=2; function(x){x=a;a=3}",
    "a=r.hex; function(x){x=a;a=3;nrow(x)*a}(a)",
    "function(x){y=x*2; y+1}(2)",
    "mean=function(x){apply(x,1,sum)/nrow(x)};mean(r.hex)",
]

badPhrases = [
    "&&",
    "||",
    "%*%",
    "ifelse",
    "cbind",
    "print",
    "apply",
    "sapply",
    "ddply",
    "var",
    "Reduce",
    "cut",
    "findInterval",
    "runif",
    "scale",
    "t",
    "seq_len",
    "seq",
    "rep_len",
    "c",
    "table",
    "unique",
    "factor",
]
phrases = [
    "function(x) { cbind( mean(x[,1]), mean(x[,2]) ) }",
    "function(x) { mean( x[,2]) }", 
    "function(x) { sd( x[,2]) }", 
    "function(x) { quantile(x[,2] , c(0.9) ) }",

    "nrow",
    "ncol",
    "length",
    "is.factor",
    "any.factor",
    "any.na",
    "isTRUE",
    "min.na.rm",
    "max.na.rm",
    "min",
    "max",
    "xorsum",
]

if DO_COMPOUND:
    phrases += phrasesCompound

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED, localhost
        SEED = h2o.setup_random_seed()
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(3)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_exec2_ddply_phrases(self):
        h2o.beta_features = True
        bucket = 'home-0xdiag-datasets'
        # csvPathname = 'standard/covtype.data'
        csvPathname = "standard/covtype.shuffled.10pct.data"

        hexKey = 'i.hex'
        parseResult = h2i.import_parse(bucket=bucket, path=csvPathname, schema='local', hex_key=hexKey)

        for resultKey, execExpr in initList:
            h2e.exec_expr(h2o.nodes[0], execExpr, resultKey=resultKey, timeoutSecs=60)

        for p in phrases:
            execExpr = "ddply(r.hex, c(2,3), " + p + ")" 
            h2e.exec_expr(h2o.nodes[0], execExpr, resultKey=None, timeoutSecs=60)

        

        

if __name__ == '__main__':
    h2o.unit_main()
