import unittest, random, sys, time
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_browse as h2b, h2o_exec as h2e, h2o_import as h2i

initList = [
    ('r.hex', 'r.hex=i.hex'),
]

DO_COMPOUND = False

# disallow lhs assigns in clauses
phrasesCompound = [
        # "a=!0; x=!0",
        # "a=0; x=0",
        # "a=1; a=2; function(x){x=a;a=3}",
        # "a=c(11,22,33,44,55,66); a[c(2,6,1),]",
        # "a=r.hex; function(x){x=a;a=3;nrow(x)*a}(a)",
        # "function(x){y=x*2; y+1}(2)",
        # "mean2=function(x){apply(x,1,sum)/nrow(x)};mean2(r.hex)",
        "r.hex[1,-1]; r.hex[1,-1]; r.hex[1,-1]",
        # "r.hex[,1]=3.3; r.hex",
        # "x=!0; x+!2",
        # "x=!0; !x+2",
        # "x= 3; r.hex[,(x > 0) & (x < 4)]",
        # "x= 3; r.hex[(x > 0) & (x < 4),]",
        # "x=0; x+2",
]

phrases = [
        "!1.23",
        "1.23",
        "!1.23<!2.34",
        "!1.23<2.34",
        "1.23<=2.34",
        "1.23<!2.34",
        "1.23<2.34",
        "1.23==2.34",
        "1.23>=2.34",
        "1.23>2.34",
        "1.23!=2.34",
        "(1.23+r.hex)-r.hex",
        "1.23+(r.hex-r.hex)",
        "apply(r.hex,2,sum)",
        "!c(!1,3,5)",
        "!c(1,!3,5)",
        "!c(1,3,!5)",
        "!c(1,3,5)",
        "c(1,3,5)",
        "cbind(c(1,2,3,4), c(5,6,7,8))",
        "factor(r.hex[,5])",
        # "function(funy){function(x){funy(x)*funy(x)}}(sgn)(-2)",
        #"function(x){x+1}(2)",
        #"function(x){y=1+2}(2)",
        #"function(x,y,z){x[]}(r.hex,1,2)",
        "is.na(r.hex)",
        "max(1,23)",
        "min(1,2)",
        "nrow(r.hex)*3",
        "r.hex[,1]==1.0",
        "!r.hex",
        "r.hex",
        "r.hex[,1]",
        "r.hex[1,]",
        "r.hex+1",
        "r.hex[,1]+1",
        "r.hex[,1]=r.hex[,1]+1",
        "r.hex[2,3]",
        "r.hex[2+4,-4]",
        "r.hex[c(1,3,5),]",
        "r.hex[,ncol(r.hex)+1]=4",
        "r.hex[nrow(r.hex),]",
        "r.hex[nrow(r.hex)-1,ncol(r.hex)-1]",
        "r.hex-r.hex",
        "runif(r.hex[,1], -1)",
        "sum(1,2)",
        "sum(1,2,3)",
        "sum(1,r.hex,3)",
        "sum(4,c(1,3,5),2,6)",
        "sum(c(1,3,5))",
        #"x<-!1",
        #"x<-1",
        #"x=!1",
        #"x=1",
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
        h2o.init(1)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_exec2_ifelse_phrases(self):
        bucket = 'smalldata'
        csvPathname = 'iris/iris2.csv'
        hexKey = 'i.hex'
        parseResult = h2i.import_parse(bucket=bucket, path=csvPathname, schema='put', hex_key=hexKey)

        exprList = []
        while (len(exprList)!=200):
            expr = random.choice(phrases) + " , " + random.choice(phrases)
            exprList.append("ifelse(1," + expr + ")")
            exprList.append("ifelse(0," + expr + ")")
            # FIX! what about TRUE/FALSE for the select or single value extracted from something?

        for resultKey, execExpr in initList:
            h2e.exec_expr(h2o.nodes[0], execExpr, resultKey=resultKey, timeoutSecs=4)

        for execExpr in exprList:
            h2e.exec_expr(h2o.nodes[0], execExpr, resultKey=None, timeoutSecs=4)

if __name__ == '__main__':
    h2o.unit_main()
