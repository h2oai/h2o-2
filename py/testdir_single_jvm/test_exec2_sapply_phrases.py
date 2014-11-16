import unittest, random, sys, time
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_browse as h2b, h2o_exec as h2e, h2o_import as h2i


DO_COMPOUND = False

phrasesCompound = [

    # use a dialetc with restricted grammar
    # 1. all functions are on their own line
    # 2. all functions only use data thru their params, or created in the function

    # "a=1; a=2; function(x){x=a;a=3}",
    # "a=r.hex; function(x){x=a;a=3;nrow(x)*a}(a)",
    # "function(x){y=x*2; y+1}(2)",
    # "mean2=function(x){apply(x,1,sum)/nrow(x)};mean2(r.hex)",
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
    "func1",
    "func2",
    "func3",
    "func4",
    "func5",
    # "func6",
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
        global SEED
        SEED = h2o.setup_random_seed()
        h2o.init(1, java_heap_GB=12)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_exec2_apply_phrases(self):
        bucket = 'home-0xdiag-datasets'
        # csvPathname = 'standard/covtype.data'
        csvPathname = "standard/covtype.shuffled.10pct.data"

        hexKey = 'i.hex'
        parseResult = h2i.import_parse(bucket=bucket, path=csvPathname, schema='local', hex_key=hexKey)


        for col in [1]:
            initList = [
                ('r.hex', 'r.hex=i.hex'),
                (None, "func1=function(x){max(x[,%s])}" % col),
                (None, "func2=function(x){a=3;nrow(x[,%s])*a}" % col),
                (None, "func3=function(x){apply(x[,%s],2,sum)/nrow(x[,%s])}" % (col, col) ),
                # (None, "function(x) { cbind( mean(x[,1]), mean(x[,%s]) ) }" % col),
                (None, "func4=function(x) { mean( x[,%s]) }" % col), 
                (None, "func5=function(x) { sd( x[,%s]) }" % col), 
                (None, "func6=function(x) { quantile(x[,%s] , c(0.9) ) }" % col),
            ]
            for resultKey, execExpr in initList:
                h2e.exec_expr(h2o.nodes[0], execExpr, resultKey=resultKey, timeoutSecs=60)

            for p in phrases:
                # execExpr = "sapply(r.hex, " + p + ")" 
                execExpr = "sapply(r.hex, " + p + ")" 
                h2e.exec_expr(h2o.nodes[0], execExpr, resultKey=None, timeoutSecs=60)

if __name__ == '__main__':
    h2o.unit_main()
