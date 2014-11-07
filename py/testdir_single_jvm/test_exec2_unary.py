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
        # ('x', 'x=r.hex[,1]; rcnt=nrow(x)-sum(is.na(x))'),
        # ('x', 'x=r.hex[,1]; total=sum(ifelse(is.na(x),0,x)); rcnt=nrow(x)-sum(is.na(x))'),
        # ('x', 'x=r.hex[,1]; total=sum(ifelse(is.na(x),0,x)); rcnt=nrow(x)-sum(is.na(x)); mean2=total / rcnt'),
        # ('x', 'x=r.hex[,1]; total=sum(ifelse(is.na(x),0,x)); rcnt=nrow(x)-sum(is.na(x)); mean2=total / rcnt; x=ifelse(is.na(x),mean2,x)'),
        ]

if 1==0:
    exprListFull = [
    ]
else:
    exprListFull = [
        'r1.hex=apply(r.hex,2,function(x){ifelse(is.na(x),0,x)})',
        'cct.hex=runif(r.hex, -1);rTrain=r.hex[cct.hex<=0.9,];rTest=r.hex[cct.hex>0.9,]',
        # 'r<n>[,0] = r0[,0] * r<n-1>[,0]',
        # 'r<n>[0,] = r1[0,] + r<n-1>[0,]',
        # 'r<n> = r1 + r<n-1>',
        # doesn't work
        # ';;',
        'r1.hex[,1]=r1.hex[,1]==1.0',
        # unsupported
        # 'r1.hex[1,]=r1.hex[1,]==1.0',
        'b.hex=runif(r3.hex[,1], -1)',
        'b.hex=runif(r3.hex[1,], -1)',
        # 'r1.hex[,1]=r1.hex[,1] + 1.3',
        # 'r<n>.hex=min(r1.hex,1+2)',
        # 'r<n>.hex=r2.hex + 1',
        # 'r<n>.hex=r3.hex + 1',
        # 'r<n>[,0] = r2[,0] / r<n-1>[,0]',
        # 'r<n>[,0] = r3[,0] - r<n-1>[,0]',

        # from h2o/src/test/java/water/exec/Expr2Test.java
        # FIX! update to template form?
        "1.23",        #  1.23
        # doesn't work
        # ",1.23 + 2.34" #  3.57
        # doesn't work
        # ",1.23 + 2.34 * 3" #  10.71, L2R eval order
        ## ",1.23 2.34"   #  Syntax error
        "1.23e0<2.34e1", #  1
        "+1.23e0<+2.34e1", #  1
        "-1.23e0<+-2.34e1", #  1
        "-1.23e000<+-2.34e100", #  1
        "-1.23e-001<+-2.34e-100", #  1
        "1.23<2.34", #  1
        "1.23<=2.34", #  1
        "1.23>2.34", #  0
        "1.23>=2.34", #  0
        "1.23==2.34", #  0
        "1.23!=2.34", #  1
        "1.23 <2.34", #  1
        "1.23 <=2.34", #  1
        "1.23 >2.34", #  0
        "1.23 >=2.34", #  0
        "1.23 ==2.34", #  0
        "1.23 !=2.34", #  1
        "1.23< 2.34", #  1
        "1.23<= 2.34", #  1
        "1.23> 2.34", #  0
        "1.23>= 2.34", #  0
        "1.23== 2.34", #  0
        "1.23!= 2.34", #  1
        "r.hex",       #  Simple ref
        # no longer legal
        ## "+(1.23,2.34)",#  prefix 3.57
        ## "+(1.23)",     #  Syntax error, not enuf args
        ## "+(1.23,2,3)", #  Syntax error, too many args
        "r.hex[2,3]",  #  Scalar selection
        ## "r.hex[2,+]",  #  Function not allowed
        "r.hex[2+4,-4]", #  Select row 6, all-cols but 4
        # "r.hex[1,-1]; r.hex[2,-2]; r.hex[3,-3]", #  Partial results are freed
        "r.hex[1,-1]; r.hex[1,-1]; r.hex[1,-1]", #  Partial results are freed
        ## "r.hex[2+3,r.hex]",#  Error: col selector has too many columns
        # "r.hex[2,]",   #  Row 2 all cols
        "r.hex[1,]",   #  Row 2 all cols
        # "r.hex[,3]",   #  Col 3 all rows
        "r.hex[,1]",   #  Col 3 all rows

        "r.hex+1",     #  Broadcast scalar over ary
        "r.hex-r.hex",
        "1.23+(r.hex-r.hex)",
        "(1.23+r.hex)-r.hex",
        "min(r.hex,1+2)",
        "is.na(r.hex)",
        "nrow(r.hex)*3",
        "r.hex[nrow(r.hex)-1,ncol(r.hex)-1]",
        # doesn't work
        # "1=2",
        # doesn't work
        # "x",
        "x=0; x+2",
        # doesn't work
        # "2+x",
        "x=1",
        "x<-1",        #  Alternative R assignment syntax
        # doesn't work
        ## "x=1;x=r.hex", #  Allowed to change types via shadowing at REPL level
        "a=r.hex",     #  Top-level assignment back to H2O.STORE
        ## "x<-+",
        # ?
        ## "(r.hex+1)<-2",
        "r.hex[nrow(r.hex),]",
        "r.hex[,ncol(r.hex)]",
        # double semi doesn't work
        # "r.hex[2,3]<-4;",
        "c(1,3,5)",
        # what is this?
        ### "function(=){x+1}(2)",
        # doesn't work?
        # "function(x,=){x+1}(2)",
        # doesn't work
        # "function(x,<-){x+1}(2)",
        # doesn't work
        # "function(x,x){x+1}(2)",
        "function(x,y,z){x[]}(r.hex,1,2)",
        # doesn't work?
        # "function(x){x[]}(2)",
        "function(x){x+1}(2)",
        # doesn't work
        ## "function(x){y=x+y}(2)",
        # doesn't work
        ## "function(x,y){y=x+y}(2)",
        # doesn't work
        # "function(x){}(2)",
        "function(x){y=x*2; y+1}(2)",
        "function(x){y=1+2}(2)",
        # doesn't work
        # "function(x){y=1+2;y=c(1,2)}",#  Not allowed to change types in inner scopes
        "sum(1,2,3)",
        "sum(c(1,3,5))",
        "sum(4,c(1,3,5),2,6)",
        "sum(1,r.hex,3)",
        # unimplemented?
        "r.hex[,c(1)]",
        "r.hex[c(1),]",
        "r.hex[c(1,3,5),]",
        "r.hex[,c(1,3,5)]",
        "a=c(11,22,33,44,55,66); a[c(2,6,1),]",
        # doesn't work
        # "function(a){a[];a=1}",
        "a=1;a=2;function(x){x=a;a=3}",
        "a=r.hex;function(x){x=a;a=3;nrow(x)*a}(a)",
        #  Higher-order function typing: fun is typed in the body of function(x)
        "function(funy){function(x){funy(x)*funy(x)}}(sgn)(-2)",
        #  Filter/selection
        "r.hex[r.hex[,1]>4,]",
        "a=c(1,2,3); a[a[,1]>10,1]",
        "apply(r.hex,2,sum)",
        # doesn't work
        # "y=5;apply(r.hex,1,function(x){x[]+y})",
        # doesn't work
        # "apply(r.hex,1,function(x){x=1;r.hex})",
        # doesn't work
        # "apply(r.hex,1,function(x){r.hex})",
        "mean2=function(x){apply(x,2,sum)/nrow(x)};mean2(r.hex)",
        # "mean2=function(x){apply(x,1,sum)/nrow(x)};mean2(r.hex)",

        #  Conditional selection; 
        "ifelse(0,1,2)",
        "ifelse(0,r.hex+1,r.hex+2)",
        "ifelse(r.hex>3,99,r.hex)",#  Broadcast selection
        "ifelse(0,+,*)(1,2)",      #  Select functions
        "(0 ? + : *)(1,2)",        #  Trinary select
        "(1? r.hex : (r.hex+1))[1,2]",#  True (vs false) test
        #  Impute the mean
        # doesn't work
        # "apply(r.hex,2,function(x){total=sum(ifelse(is.na(x),0,x)); rcnt=nrow(x)-sum(is.na(x)); mean2=total / rcnt; ifelse(is.na(x),mean2,x)})",
        "factor(r.hex[,5])",

        #  Slice assignment & map
        "r.hex[,1]",
        "r.hex[1,]",
        "r.hex[,1]+1",
        # unimplemented
        # "r.hex[1,]+1",
        "r.hex[,1]=3.3;r.hex",  #  Replace a col with a constant
        # "r.hex[1,]=3.3;r.hex",
        "r.hex[,1]=r.hex[,1]+1",#  Replace a col
        # "r.hex[1,]=r.hex[1,]+1",
        "r.hex[,ncol(r.hex)+1]=4",#  Extend a col
        # can't do arith on the row
        # "r.hex[nrow(r.hex)+1,]=4",
        "a=ncol(r.hex); r.hex[,c(a+1,a+2)]=5",#  Extend two cols
        # doesn't work
        # "table(r.hex)",
        # doesn't work. wants integer
        # "table(r.hex[,1])",


        # "r.hex[r.hex[,2]>4,]=-99",
        # "r.hex[2,]=r.hex[7,]",
        # "r.hex[c(1,3,5),1] = r.hex[c(2,4,6),2]",
        # "r.hex[c(1,3,5),1] = r.hex[c(2,4),2]",
        # "map()",
        # "map(1)",
        # "map(+,r.hex,1)",
        # "map(+,1,2)",
        # "map(function(x){x[];1},r.hex)",
        # "map(function(a,b,d){a+b+d},r.hex,r.hex,1)",
        # "map(function(a,b){a+ncol(b)},r.hex,r.hex)",

        "a=0;x=0",     #  Delete keys from global scope
        ]

# concatenate a lot of random choices to make life harder

exprList = []
for i in range(10):
    expr = ""
    for j in range(1):
        expr += "z.hex=" + random.choice(exprListFull) + ";"
        # expr += random.choice(exprListFull) + ";"
    exprList.append(expr)
        

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

    def test_exec2_unary(self):
        bucket = 'home-0xdiag-datasets'
        csvPathname = 'airlines/year2013.csv'
        hexKey = 'i.hex'
        parseResult = h2i.import_parse(bucket=bucket, path=csvPathname, schema='put', hex_key=hexKey)

        for resultKey, execExpr in initList:
            h2e.exec_expr(h2o.nodes[0], execExpr, resultKey=None, timeoutSecs=10)
        start = time.time()
        # h2e.exec_expr_list_rand(len(h2o.nodes), exprList, 'r1.hex', maxTrials=200, timeoutSecs=10)
        h2e.exec_expr_list_rand(len(h2o.nodes), exprList, None, maxTrials=200, timeoutSecs=30, allowEmptyResult=True, nanOkay=True)

        h2o.check_sandbox_for_errors()
        print "exec end on ", "operators" , 'took', time.time() - start, 'seconds'


if __name__ == '__main__':
    h2o.unit_main()
