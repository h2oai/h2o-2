import unittest, random, sys, time
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_browse as h2b, h2o_exec as h2e, h2o_import as h2i

initList = [
        # ('r1.hex', 'r1.hex=c(1.3,0,1,2,3,4,5)'),
        # ('r2.hex', 'r2.hex=c(2.3,0,1,2,3,4,5)'),
        # ('r3.hex', 'r3.hex=c(4.3,0,1,2,3,4,5)'),
        ('r.hex', 'r.hex=i.hex'),
        ('r1.hex', 'r1.hex=i.hex'),
        ('r2.hex', 'r2.hex=i.hex'),
        ('r3.hex', 'r3.hex=i.hex'),
        ('x', 'x=c(1)'),
        ('y', 'y=c(1)'),
        # not supported. should? also row vector assign?
        # ('r.hex',  'r.hex[1,]=3.3'),

        # ('x', 'x=r.hex[,1]; rcnt=nrow(x)-sum(is.na(x))'),
        # ('x', 'x=r.hex[,1]; total=sum(ifelse(is.na(x),0,x)); rcnt=nrow(x)-sum(is.na(x))'),
        # ('x', 'x=r.hex[,1]; total=sum(ifelse(is.na(x),0,x)); rcnt=nrow(x)-sum(is.na(x)); mean2=total / rcnt'),
        # ('x', 'x=r.hex[,1]; total=sum(ifelse(is.na(x),0,x)); rcnt=nrow(x)-sum(is.na(x)); mean2=total / rcnt; x=ifelse(is.na(x),mean2,x)'),
        ]

# apply: return vector or array or list of values..applying function to margins of array or matrix
# margins: either rows(1), columns(2) or both(1:2)
# "apply(r.hex,2,function(x){total=sum(ifelse(is.na(x),0,x)); rcnt=nrow(x)-sum(is.na(x)); mean2=0.0; ifelse(is.na(x),mean2,x)})",
# doesn't work. Should work according to earl
# 'r.hex[is.na(r.hex)]<-0',
# works
# 'r1.hex=apply(r.hex,2,function(x){ifelse(is.na(x),0,x)})',
# "mean2=function(x){apply(x,2,sum)/nrow(x)};mean2(r.hex)",

exprListFull = [
    # "quantile(r.hex[,4],c(0.001,.05,0.3,0.55,0.7,0.95,0.99))",
    "quantile(r.hex[,4],c(0.000))",
    "quantile(r.hex[,4],c(0.000))",
    "quantile(r.hex[,4],c(0.000))",

    "quantile(r.hex[,4],c(0.001))",
    "quantile(r.hex[,4],c(0.001))",
    "quantile(r.hex[,4],c(0.001))",

    "quantile(r.hex[,4],c(0.000,0.000))",
    "quantile(r.hex[,4],c(0.000,0.000))",
    "quantile(r.hex[,4],c(0.000,0.000))",

    "quantile(r.hex[,4],c(0.001,0.001))",
    "quantile(r.hex[,4],c(0.001,0.001))",
    "quantile(r.hex[,4],c(0.001,0.001))",

    "quantile(r.hex[,4],c(0.001,0.001,0.001))",
    "quantile(r.hex[,4],c(0.001,0.001,0.001))",
    "quantile(r.hex[,4],c(0.001,0.001,0.001))",

    "quantile(r.hex[,4],c(0.05))",
    "quantile(r.hex[,4],c(0.05))",
    "quantile(r.hex[,4],c(0.05))",

    "quantile(r.hex[,4],c(0.05,0.05))",
    "quantile(r.hex[,4],c(0.05,0.05))",
    "quantile(r.hex[,4],c(0.05,0.05))",

    "quantile(r.hex[,4],c(0.05,0.05,0.05))",
    "quantile(r.hex[,4],c(0.05,0.05,0.05))",
    "quantile(r.hex[,4],c(0.05,0.05,0.05))",

    "quantile(r.hex[,4],c(0.05,.001))",
    "quantile(r.hex[,4],c(0.05,.001))",
    "quantile(r.hex[,4],c(0.05,.001))",

    "quantile(r.hex[,4],c(0.05,.000))",
    "quantile(r.hex[,4],c(0.05,.000))",
    "quantile(r.hex[,4],c(0.05,.000))",

    "quantile(r.hex[,4],c(0.000,.05))",
    "quantile(r.hex[,4],c(0.000,.05))",
    "quantile(r.hex[,4],c(0.000,.05))",

    "quantile(r.hex[,4],c(0.001,.05))",
    "quantile(r.hex[,4],c(0.001,.05))",
    "quantile(r.hex[,4],c(0.001,.05))",


    "quantile(r.hex[,4],c(0.001,.05))",
    "quantile(r.hex[,4],c(0.001,.05,0.3))",
    "quantile(r.hex[,4],c(0.001,.05,0.3,0.55))",
    "quantile(r.hex[,4],c(0.001,.05,0.3,0.55,0.7))",
    "quantile(r.hex[,4],c(0.001,.05,0.3,0.55,0.7,0.95))",
    "quantile(r.hex[,4],c(0.001,.05,0.3,0.55,0.7,0.95,0.99))",
    "r3.hex[,1] = cos(r.hex[,1])",
    "r3.hex[,1] = sin(r.hex[,1])",
    "r3.hex[,1] = tan(r.hex[,1])",
    "r3.hex[,1] = acos(r.hex[,1])",
    "r3.hex[,1] = asin(r.hex[,1])",
    "r3.hex[,1] = atan(r.hex[,1])",
    "r3.hex[,1] = cosh(r.hex[,1])",
    "r3.hex[,1] = sinh(r.hex[,1])",
    "r3.hex[,1] = tanh(r.hex[,1])",
    "r3.hex[,1] = abs(r.hex[,1])",
    "r3.hex[,1] = sgn(r.hex[,1])",
    "r3.hex[,1] = sqrt(r.hex[,1])",
    "r3.hex[,1] = ceil(r.hex[,1])",
    "r3.hex[,1] = floor(r.hex[,1])",
    "r3.hex[,1] = log(r.hex[,1])",
    "r3.hex[,1] = log(r.hex[,1]+1)",
    "r3.hex[,1] = exp(r.hex[,1])",
    "r3.hex[,1] = is.na(r.hex[,1])",
    "1.23",
    " 1.23 + 2.34",
    " 1.23 + 2.34 * 3",
    # " 1.23 2.34",
    "1.23 < 2.34",
    "1.23 <=2.34",
    "1.23 > 2.34",
    "1.23 >=2.34",
    "1.23 ==2.34",
    "1.23 !=2.34",
    "1 & 2",
    # right now my routines don't like nan as result
    # "NA&0",
    # "0&NA",
    # "NA&1",
    # "1&NA",
    # "1|NA",
    "1&&2",
    # "1||0",
    # "NA||1",
    # "NA||0",
    # "0||NA",
    "!1",
    "(!)(1)",
   #  "(!!)(1)",
    "-1",
    "-(1)",
    # "(-)(1)",
    "-T",
    # "* + 1",
    # "+(1.23,2.34)",
    "+(1.23)",
    # "1=2",
    "x",
    "x+2",
    "2+x",
    "x=1",
    "x<-1",
    "x=3;y=4",
    "x=mean",
    "x=mean=3",
    "x=mean(c(3))",
    # "x=mean+3",
    "r.hex",
    "r.hex[2,3]",
    # "r.hex[2,+]",
    "r.hex[2+4,-4]",
    "r.hex[1,-1]; r.hex[2,-2]; r.hex[3,-3]",
    # "r.hex[2+3,r.hex]",
    "r.hex[2,]",
    "r.hex[,3]",
    "r.hex+1",
    "r.hex-r.hex",
    "1.23+(r.hex-r.hex)",
    "(1.23+r.hex)-r.hex",
    "min(r.hex,1+2)",
    "max(r.hex,1+2)",
    # "min.na.rm(r.hex,NA)",
    # "max.na.rm(r.hex,NA)",
    # "min.na.rm(c(NA, 1), -1)",
    # "max.na.rm(c(NA, 1), -1)",
    # my libraries don't like infinities
    # "max(c(Inf,1),  2 )",
    # "min(c(Inf,1),-Inf)",
    "is.na(r.hex)",
    "sum(is.na(r.hex))",
    "nrow(r.hex)*3",
    "r.hex[nrow(r.hex)-1,ncol(r.hex)-1]",
    "x=1;x=r.hex",
    "a=r.hex",
    # "(r.hex+1)<-2",
    # "r.hex[nrow(r.hex=1),]",
    # "r.hex[{r.hex=10},]",
    "r.hex[2,3]<-4;",
    "c(1,3,5)",
    "r.hex[,c(1,3,5)]",
    "r.hex[c(1,3,5),]",
    "a=c(11,22,33,44,55,66); a[c(2,6,1),]",
    "c(1,0)&c(2,3)",
    # "c(2,NA)&&T",
    "-(x = 3)",
    "x<-+",
    # "x<-+;x(2)",
    "x<-+;x(1,2)",
    "x<-*;x(2,3)",
    "x=c(0,1);!x+1",
    "x=c(1,-2);-+---x",
    "x=c(1,-2);--!--x",
    "!(y=c(3,4))",
    "!x!=1",
    "(!x)!=1",
    "1+x^2",
    "x=c(1);1+x^2",
    # these currently don't work. although they are legit R
    # "1+x**2",
    # "x=c(1);1+x**2",
    "x=c(1); y=c(1); x + 2/y",
    "x=c(1); y=c(1); x + (2/y)",
    "x=c(1); y=c(1); -x + y",
    "x=c(1); y=c(1); -(x + y)",
    "x=c(1); y=c(1); -x % y",
    "x=c(1); y=c(1); -(x % y)",
    "T|F&F",
    "T||F&&F",
    # h2o doesn't like this
    # "function(=){x+1}(2)",
    # "function(x,=){x+1}(2)",
    # "function(x,<-){x+1}(2)",
    # "function(x,x){x+1}(2)",
    "function(x,y,z){x[]}(r.hex,1,2)",
    # h2o doesn't like this
    # "function(x){x[]}(2)",
    "function(x){x+1}(2)",
    "function(x){y=x+y}(2)",
    # "function(x){}(2)",
    "function(x){y=x*2; y+1}(2)",
    "function(x){y=1+2}(2)",
    # "function(x){y=1+2;y=c(1,2)}",
    "a=function(x) x+1; 7",
    "a=function(x) {x+1}; 7",
    "a=function(x) {x+1; 7}",
    "c(1,c(2,3))",
    # "a=c(1,Inf);c(2,a)",
    "sum(1,2,3)",
    "sum(c(1,3,5))",
    "sum(4,c(1,3,5),2,6)",
    "sum(1,r.hex,3)",
    # "sum(c(NA,-1,1))",
    # "sum.na.rm(c(NA,-1,1))",
    # can't change type?
    # "function(a){a[];a=1}",
    "a=1;a=2;function(x){x=a;a=3}",
    "a=r.hex;function(x){x=a;a=3;nrow(x)*a}(a)",
    "a=r.hex;a[,1]=(a[,1]==8)",
    "function(funy){function(x){funy(x)*funy(x)}}(sgn)(-2)",
    "r.hex[r.hex[,4]>30,]",
    "a=c(1,2,3);a[a[,1]>10,1]",
    # "sapply(a,sum)[1,1]",
    "apply(r.hex,2,sum)",
    "y=5;apply(r.hex,2,function(x){x[]+y})",
    # "apply(r.hex,2,function(x){x=1;r.hex})",
    # "apply(r.hex,2,function(x){r.hex})",
    "apply(r.hex,2,function(x){sum(x)/nrow(x)})",
    "mean2=function(x){apply(x,2,sum)/nrow(x)};mean2(r.hex)",
    "ifelse(0,1,2)",
    "ifelse(0,r.hex+1,r.hex+2)",
    "ifelse(r.hex>3,3,r.hex)",
    "ifelse(0,+,*)(1,2)",
    "(0 ? + : *)(1,2)",
    "(1? r.hex : (r.hex+1))[1,2]",
    "apply(r.hex,2,function(x){total=sum(ifelse(is.na(x),0,x)); rcnt=nrow(x)-sum(is.na(x)); mean2=total / rcnt; ifelse(is.na(x),mean2,x)})",
    "factor(r.hex[,5])",
    "r.hex[,2]",
    "r.hex[,2]+1",
    "r.hex[,3]=3.3;r.hex",
    "r.hex[,3]=r.hex[,2]+1",
    "r.hex[,ncol(r.hex)+1]=4",
    "a=ncol(r.hex);r.hex[,c(a+1,a+2)]=5",
    # "table(r.hex)",
    "table(r.hex[,5])",
    "table(r.hex[,c(2,7)])",
    "table(r.hex[,c(2,9)])",
    "a=cbind(c(1,2,3), c(4,5,6))",
    "a[,1] = factor(a[,1])",
    "is.factor(a[,1])",
    "isTRUE(c(1,3))",
    "a=1;isTRUE(1)",
    "a=c(1,2);isTRUE(a)",
    "isTRUE(min)",
    # "seq_len(0)",
    # "seq_len(-1)",
    "seq_len(10)",
    "3 < 4 |  F &  3 > 4",
    "3 < 4 || F && 3 > 4",
    "r.hex[,4] != 29 || r.hex[,2] < 305 && r.hex[,2] < 81",
    "quantile(r.hex[,4],c(0.001,.05,0.3,0.55,0.7,0.95,0.99))",
    "quantile(seq_len(10),seq_len(10)/10)",
    "quantile(runif(seq_len(10000), -1), seq_len(10)/10)",
    # "quantile(r.hex[,4],c(0.001,.05,0.3,0.55,0.7,0.95,0.99))",
    # problem with 0?
    "quantile(r.hex[,4],c(0,.05,0.3,0.55,0.7,0.95,0.99))",
    # "ddply(r.hex,r.hex,sum)",
    # "ddply(r.hex,seq_len(10000),sum)",
    # "ddply(r.hex,NA,sum)",
    # "ddply(r.hex,c(1,NA,3),sum)",
    "a=0;x=0;y=0",
    # takes more than 30 seconds?
    # "ddply(r.hex,c(1,3,3),sum)",
    ]

# concatenate a lot of random choices to make life harder
if 1==0:
    exprList = []
    for i in range(10):
        expr = ""
        for j in range(1):
            expr += "z.hex=" + random.choice(exprListFull) + ";"
            # expr += random.choice(exprListFull) + ";"
        exprList.append(expr)

exprList = exprListFull

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = h2o.setup_random_seed()
        h2o.init(1, java_heap_GB=14)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_exec2_operators(self):
        bucket = 'home-0xdiag-datasets'
        # csvPathname = 'airlines/year2013.csv'
        csvPathname = 'standard/covtype.data'
        hexKey = 'i.hex'
        parseResult = h2i.import_parse(bucket=bucket, path=csvPathname, schema='put', hex_key=hexKey)

        for resultKey, execExpr in initList:
            h2e.exec_expr(h2o.nodes[0], execExpr, resultKey=None, timeoutSecs=10)
        start = time.time()
        # h2e.exec_expr_list_rand(len(h2o.nodes), exprList, 'r1.hex', maxTrials=200, timeoutSecs=10)
        for execExpr in exprList:
            h2e.exec_expr(h2o.nodes[0], execExpr, resultKey=None, timeoutSecs=30)

        h2o.check_sandbox_for_errors()
        print "exec end on ", "operators" , 'took', time.time() - start, 'seconds'


if __name__ == '__main__':
    h2o.unit_main()
