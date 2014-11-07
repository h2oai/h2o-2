import unittest, random, sys, time, re
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_browse as h2b, h2o_exec as h2e, h2o_import as h2i

initList = [
        ('r.hex', 'r.hex=i.hex'),
        ]

DO_IFELSE = False
DO_CAN_RETURN_NAN = False
DO_FAIL1 = False
DO_TERNARY = False
DO_APPLY = True
DO_FUNCTION = False
DO_FORCE_LHS_ON_MULTI = True

exprList = [
    "round(r.hex[,1],0)",
    "round(r.hex[,1],1)",
    "round(r.hex[,1],2)",
    # "signif(r.hex[,1],-1)",
    # "signif(r.hex[,1],0)",
    "signif(r.hex[,1],1)",
    "signif(r.hex[,1],2)",
    "signif(r.hex[,1],22)",
    "trunc(r.hex[,1])",
    "trunc(r.hex[,1])",
    "trunc(r.hex[,1])",
    "trunc(r.hex[,1])",

    'x= 3; r.hex[(x > 0) & (x < 4),]',    # all x values between 0 and 1
    'x= 3; r.hex[,(x > 0) & (x < 4)]',    # all x values between 0 and 1
    # 'z = if (any(r3.hex == 0) || any(r4.hex == 0)), "zero encountered"',

    # FALSE and TRUE don't exist?
    # 'x <- c(NA, FALSE, TRUE)',

    # 'names(x) <- as.character(x)'
    # outer(x, x, "&")## AND table
    # outer(x, x, "|")## OR  table
    "1.23",
    "!1.23",

    "1.23<2.34",
    "!1.23<2.34",
    "!1.23<!2.34",
    "1.23<!2.34",

    "1.23<=2.34",
    "1.23>2.34",
    "1.23>=2.34",
    "1.23==2.34",
    "1.23!=2.34",

    "r.hex",
    "!r.hex",

    # Not supported
    # "+(1.23,2.34)",
    "x=0; x+2",
    "x=!0; !x+2",
    "x=!0; x+!2",

    "x=1",
    "x=!1",

    "x<-1",
    "x<-!1",
    "c(1,3,5)",
    "!c(1,3,5)",
    "!c(!1,3,5)",
    "!c(1,!3,5)",
    "!c(1,3,!5)",

    "a=0; x=0",
    "a=!0; x=!0",

    "r.hex[2,3]",
    # "r.hex[!2,3]",
    # no cols selectd
    # "r.hex[2,!3]",

    "r.hex[2+4,-4]",
    "r.hex[1,-1]; r.hex[1,-1]; r.hex[1,-1]",
    "r.hex[1,]",
    "r.hex+1",
    "r.hex[,1]",
    "r.hex[,1]+1",

    "r.hex-r.hex",
    "1.23+(r.hex-r.hex)",
    "(1.23+r.hex)-r.hex",

    "is.na(r.hex)",

    "nrow(r.hex)*3",
    "r.hex[nrow(r.hex)-1,ncol(r.hex)-1]",
    "r.hex[nrow(r.hex),]",
    "r.hex[,ncol(r.hex)+1]=4",
    "r.hex[,1]=3.3; r.hex",
    "r.hex[,1]=r.hex[,1]+1",


    # doesn't work
    # "cbind(c(1), c(2), c(3))",
    # "cbind(c(1,2,3), c(4,5,6))",
    # "cbind(c(1,2,3), c(4,5,6), c(7,8,9))",
    # "cbind(c(1,2,3,4), c(5,6,7))",
    # "cbind(c(1,2,3), c(4,5,6,7))",
    "cbind(c(1,2,3,4), c(5,6,7,8))",

    "r.hex[c(1,3,5),]",
    "a=c(11,22,33,44,55,66); a[c(2,6,1),]",

    # fails?
    # "a=c(1,2,3); a[a[,1]>10,1]",



    "sum(1,2)",
    "sum(1,2,3)",
    "sum(c(1,3,5))",
    "sum(4,c(1,3,5),2,6)",
    "sum(1,r.hex,3)",

    "min(1,2)",
    # doesn't work
    # "min(1,2,3)",
    # doesn't work. only 2 params?
    # "min(c(1,3,5))",
    # doesn't work. only 2 params?
    # "min(4,c(1,3,5),2,6)",
    # doesn't work
    # "min(1,r.hex,3)",

    "max(1,23)",
    # doesn't work
    # Passed 3 args but expected 2
    # "max(1,2,3)",
    # doesn't work
    # "max(c(1,3,5))",

    # doesn't work
    # Passed 4 args but expected 2
    # "max(4,c(1,3,5),2,6)",
    # doesn't work
    # "max(1,r.hex,3)",

    "factor(r.hex[,5])",
    "r.hex[,1]==1.0",
    "runif(r.hex[,1], -1)",
    "r.hex[,3]=4",

    ]


if DO_APPLY:
    exprList += [
        "apply(r.hex,1,sum)",
        "apply(r.hex,2,sum)",
    ]

if DO_FUNCTION:
    exprList += [
        # doesn't work
        # "crnk=function(x){99}",
        # "crk=function(x){99}",
        "crunk=function(x){x+99}",
        # "function(x){x+99}",
        # "crunk=function(x){99}; r.hex[,3]=4",
        "function(x,y,z){x[]}(r.hex,1,2)",
        "function(x){x+1}(2)",
        "function(x){y=x*2; y+1}(2)",
        "function(x){y=1+2}(2)",
        "function(funy){function(x){funy(x)*funy(x)}}(sgn)(-2)",
        "a=1; a=2; function(x){x=a;a=3}",
        "a=r.hex; function(x){x=a;a=3;nrow(x)*a}(a)",
        # "mean2=function(x){apply(x,1,sum)/nrow(x)};mean2(r.hex)",
        # "mean2=function(x){apply(x,2,sum)/nrow(x)};mean2(r.hex)",
        "mean2=function(x){99/nrow(x)};mean2(r.hex)",
        "mean2=function(x){99/nrow(x)}",
        # what happens if you rename a function in a single string
    ]
# FIX! should add ternary here?
# ifelse does all 3 params
# ? doesn't do the else if true
# we don't support the split if/else
if DO_TERNARY:
    exprList += [
        # do we really care about this case
        # "(0 ? + : *)(1,2)",
        # "0 ? + : * (1, 2)",
        "1 ? r.hex : (r.hex+1)",
        "1 ? (r.hex+1) : r.hex",

        # don't do these harder ternary for now
        #"(1 ? r.hex : (r.hex+1))[1,2]",
        # "apply(r.hex,2, function(x){x==-1 ? 1 : x})",
        "0 ? 1 : 2",
        "0 ? r.hex+1 : r.hex+2",
        "r.hex>3 ? 99 : r.hex",
    ]

if DO_IFELSE:
    exprList += [
        "apply(r.hex,2,function(x){ifelse(x==-1,1,x)})",
        "ifelse(0,1,2)",
        "ifelse(0,r.hex+1,r.hex+2)",
        "ifelse(r.hex>3,99,r.hex)",
        "ifelse(0,+,*)(1,2)",
    ]    

if DO_CAN_RETURN_NAN:
    exprList += [
        "r.hex[r.hex[,1]>4,]",
    ]

if DO_FAIL1:
    exprList += [
        "a=ncol(r.hex); r.hex[,c(a+1,a+2)]=5",
    ]

# concatenate some random choices to make life harder
exprBigList = []
for i in range(1000):
    # expr = ""
    # concatNum = random.randint(1,2)
    # expr = "crunk=function(x){x+98};"
    expr = ""
    # expr = "function(x){x+98};"
    concatNum = random.randint(1,3)
    for j in range(concatNum):
        randExpr = random.choice(exprList)
        if DO_FORCE_LHS_ON_MULTI:
            # lhs =? 
            if re.search("=(?!=)", randExpr):
                expr += randExpr + ";"
            else:
                expr += "d=" + randExpr + ";"
        else:
            expr += randExpr + ";"

    assert expr!="r"
    exprBigList.append(expr)
        

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

    def test_exec2_operators2(self):
        bucket = 'smalldata'
        csvPathname = 'iris/iris2.csv'
        hexKey = 'i.hex'
        parseResult = h2i.import_parse(bucket=bucket, path=csvPathname, schema='put', hex_key=hexKey)

        for resultKey, execExpr in initList:
            h2e.exec_expr(h2o.nodes[0], execExpr, resultKey=resultKey, timeoutSecs=4)

        start = time.time()
        h2e.exec_expr_list_rand(len(h2o.nodes), exprList, None, maxTrials=200, timeoutSecs=10, allowEmptyResult=True)

        # now run them just concatenating each time. We don't do any template substitutes, so don't need
        # exec_expr_list_rand()
        
        bigExecExpr = ""
        for execExpr in exprBigList:
            h2e.exec_expr(h2o.nodes[0], execExpr, resultKey=None, timeoutSecs=10)

        h2o.check_sandbox_for_errors()
        print "exec end on ", "operators" , 'took', time.time() - start, 'seconds'


if __name__ == '__main__':
    h2o.unit_main()
