import unittest, random, sys, time
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_browse as h2b, h2o_exec as h2e, h2o_import as h2i

initList = [
    ('rhex', 'rhex=i.hex'),
]

DO_COMPOUND = True
DO_IRIS = True

# since a function definition has to be on it's own line, you can't redefine functions within a function
#        "a=rhex; function(x){x=a;a=3;nrow(x)*a}(a)",
#        "function(x){y=x*2; y+1}(2)",
#        "a=1; a=2; function(x){x=a;a=3}",
#        "mean2=function(x){apply(x,1,sum)/nrow(x)};mean2(rhex)",
phrasesCompound = [
        "aa=!0; x=!0",
        "aa=0; x=0",
        "bb=c(11,22,33,44,55,66); bb[c(2,6,1),]",
        "rhex[1,-1]; rhex[1,-1]; rhex[1,-1]",
        # can't assign to rhex param?
        # "rhex[,1]=3.3; rhex",
        "x=!0; x+!2",
        "x=!0; !x+2",
        "x= 3; rhex[,(x > 0) & (x < 4)]",
        "x= 3; rhex[(x > 0) & (x < 4),]",
        "x=0; x+2",
]

phrases = [
        "round(rhex[,1],0)",
        "round(rhex[,1],1)",
        "round(rhex[,1],2)",
        # "signif(rhex[,1],-1)",
        # "signif(rhex[,1],0)",
        "signif(rhex[,1],1)",
        "signif(rhex[,1],2)",
        "signif(rhex[,1],22)",
        "trunc(rhex[,1])",
        "trunc(rhex[,1])",
        "trunc(rhex[,1])",
        "trunc(rhex[,1])",

        "ifelse(1,0,2)",
        "ifelse(0,0,2)",
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
        "(1.23+rhex)-rhex",
        "1.23+(rhex-rhex)",
        "apply(rhex,2,sum)",
        "!c(!1,3,5)",
        "!c(1,!3,5)",
        "!c(1,3,!5)",
        "!c(1,3,5)",
        "c(1,3,5)",
        "cbind(c(1,2,3,4), c(5,6,7,8))",
        "factor(rhex[,5])",
        # "function(funy){function(x){funy(x)*funy(x)}}(sgn)(-2)",
        # "function(x){x+1}(2)",
        # "function(x){y=1+2}(2)",
        # "function(x,y,z){x[]}(rhex,1,2)",
        "is.na(rhex)",
        "max(1,23)",
        "min(1,2)",
        "nrow(rhex)*3",

        "rhex[,1]==1.0",
        "!rhex",
        "rhex",
        "rhex[,1]",
        "rhex[1,]",
        "rhex+1",
        "rhex[,1]+1",
        # can't assign to rhex param
        # "rhex[,1]=rhex[,1]+1",
        "rhex[2,3]",
        "rhex[2+4,-4]",
        "rhex[c(1,3,5),]",
        # can't assign to rhex param
        # "rhex[,ncol(rhex)+1]=4",
        "rhex[nrow(rhex),]",
        "rhex[nrow(rhex)-1,ncol(rhex)-1]",
        "rhex-rhex",
        "runif(rhex[,1], -1)",
        "sum(1,2)",
        "sum(1,2,3)",
        "sum(1,rhex,3)",
        "sum(4,c(1,3,5),2,6)",
        "sum(c(1,3,5))",
        "x<-!1",
        "x<-1",
        "x=!1",
        "x=1",
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

    def test_NOPASS_exec2_function_phrases(self):
        if DO_IRIS:
            bucket = 'smalldata'
            csvPathname = 'iris/iris2.csv'
        else:
            bucket = 'home-0xdiag-datasets'
            csvPathname = 'standard/covtype.data'

        hexKey = 'i.hex'
        parseResult = h2i.import_parse(bucket=bucket, path=csvPathname, schema='put', hex_key=hexKey)

        exprList = []
        bigExprList = []

        while (len(exprList)!=500):
            # expr = random.choice(phrases) + " ; " + random.choice(phrases)
            expr = random.choice(phrases) + " ;"
            # h2o doesn't support ternary within a function?
            # exprList.append("1 ? " + expr)
            # exprList.append("0 ? " + expr)
            exprList.append(expr)
            # FIX! what about TRUE/FALSE for the select or single value extracted from something?

        for resultKey, execExpr in initList:
            execExpr = "func1 = function(x,y,z,rhex){ a=" + execExpr + " }"
            h2e.exec_expr(h2o.nodes[0], execExpr, resultKey=resultKey, timeoutSecs=4)

        for execExpr in exprList:
            execExpr = "func2 = function(x,y,z,rhex){ a=" + execExpr + " }"
            h2e.exec_expr(h2o.nodes[0], execExpr, resultKey=None, timeoutSecs=4)
            execExpr = "func2(0,0,0,i.hex)"
            h2e.exec_expr(h2o.nodes[0], execExpr, resultKey=None, timeoutSecs=4)

        # now do some double concats of the expressions created
        for j in range (50):
            execExpr = "a=" + random.choice(exprList) + "b=" + random.choice(exprList)
            execExpr = "func3 = function(x,y,z,rhex){ " + execExpr + " }"
            h2e.exec_expr(h2o.nodes[0], execExpr, resultKey=None, timeoutSecs=4)
            execExpr = "func3(0,0,0,i.hex)"
            h2e.exec_expr(h2o.nodes[0], execExpr, resultKey=None, timeoutSecs=4)

        # now do some triple concats of the expressions created
        for j in range (50):
            execExpr = "a=" + random.choice(exprList) + "b=" + random.choice(exprList) + "d=" + random.choice(exprList)
            execExpr = "func4 = function(x,y,z,rhex){ " + execExpr + " }"
            h2e.exec_expr(h2o.nodes[0], execExpr, resultKey=None, timeoutSecs=4)
            execExpr = "func4(0,0,0,i.hex)"
            h2e.exec_expr(h2o.nodes[0], execExpr, resultKey=None, timeoutSecs=4)
        

        

if __name__ == '__main__':
    h2o.unit_main()
