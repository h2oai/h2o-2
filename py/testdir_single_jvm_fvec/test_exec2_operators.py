import unittest, random, sys, time
sys.path.extend(['.','..','py'])
import h2o, h2o_browse as h2b, h2o_exec as h2e, h2o_hosts, h2o_import as h2i

# new ...ability to reference cols
# src[ src$age<17 && src$zip=95120 && ... , ]
# can specify values for enums ..values are 0 thru n-1 for n enums
print "FIX!: need to test the && and || reduction operators"
DO_FUNCTION = False
initList = [
        # ('r.1', 'r.1=c(1.3,0,1,2,3,4,5)'),
        # ('r2.hex', 'r2.hex=c(2.3,0,1,2,3,4,5)'),
        # ('r3.hex', 'r3.hex=c(4.3,0,1,2,3,4,5)'),
        ('r.1', 'r.1=i.hex'),
        ('r.1', 'r.1=i.hex'),
        ('r2.hex', 'r2.hex=i.hex'),
        ('r3.hex', 'r3.hex=i.hex'),
        # not supported. should? also row vector assign?
        # ('r.1',  'r.1[1,]=3.3'),

        # ('x', 'x=r.1[,1]; rcnt=nrow(x)-sum(is.na(x))'),
        # ('x', 'x=r.1[,1]; total=sum(ifelse(is.na(x),0,x)); rcnt=nrow(x)-sum(is.na(x))'),
        # ('x', 'x=r.1[,1]; total=sum(ifelse(is.na(x),0,x)); rcnt=nrow(x)-sum(is.na(x)); mean=total / rcnt'),
        # ('x', 'x=r.1[,1]; total=sum(ifelse(is.na(x),0,x)); rcnt=nrow(x)-sum(is.na(x)); mean=total / rcnt; x=ifelse(is.na(x),mean,x)'),
        ]

# apply: return vector or array or list of values..applying function to margins of array or matrix
# margins: either rows(1), columns(2) or both(1:2)
# "apply(r.1,2,function(x){total=sum(ifelse(is.na(x),0,x)); rcnt=nrow(x)-sum(is.na(x)); mean=0.0; ifelse(is.na(x),mean,x)})",
# doesn't work. Should work according to earl
# 'r.1[is.na(r.1)]<-0',
# works
# 'r.1=apply(r.1,2,function(x){ifelse(is.na(x),0,x)})',
# "mean=function(x){apply(x,2,sum)/nrow(x)};mean(r.1)",

deepIfElse = ""
closeParen = 0
for i in range(20):
    # two, so we complement
    deepIfElse += "ifelse(1,0,"
    deepIfElse += "ifelse(0,1,"
    closeParen += 1
# now add all the closeParens you need
deepIfElse += "0"
for i in range(closeParen):
    deepIfElse += '))'

exprListFull = [
    deepIfElse,
    # this is supposed to work on vendavo branch?
    # "apply(r.1, 1, function(x) { x+1 })",
    # fails if row based ..okay if col base
    # "apply(r.1, 1, function(x) { mean(x) })",
    # "apply(r.1, 1, function(x) { sd(x) })",
    # "apply(r.1, 1, function(x) { sd(x)/mean(x) })",

    "apply(r.1, 2, function(x) { mean(x) })",
    "apply(r.1, 2, function(x) { sd(x) })",
    "apply(r.1, 2, function(x) { sd(x)/mean(x) })",

    "ddply(r.1,c(3),nrow)",
    # More complex multi-return
    # ddply can only return one thing
    # "ddply(r.1,c(3),function(x) {c(mean(x[,2]),mean(x[,3]))})",
    "ddply(r.1,c(7),nrow)",

    "s1=c(1); s2=c(2); s3=c(3); s4=c(4); s5=s1+s2+s3+s4;"
    "s.hex = r.1[!is.na(r.1[,13]),]",
    "apply(r.1,2,function(x){total=sum(ifelse(is.na(x),0,x)); rcnt=nrow(x)-sum(is.na(x)); mean=total / rcnt; ifelse(is.na(x),mean,x)})",
    "s.hex = r.1[!is.na(r.1[,13]),]",
    'r.1=apply(r.1,2,function(x){ifelse(is.na(x),0,x)})',
    'cct.hex=runif(r.1);rTrain=r.1[cct.hex<=0.9,];rTest=r.1[cct.hex>0.9,]',
    #******************************************************************************

    # says you can't use col 0
    # 'r.1[,0] = r.1[,0] * r2.hex[,1]',
    # 'r.1[,0] = r.1[,0] + r2.hex[,1]',

    # says arrays must be same size
    # 'r.1 = r.1 + r2.hex[,1]',

    # bad exception due to different sizes
    # 'r.1 = r.1 + r2.hex[1,]',

    'r.1[,1]=r.1[,1]==1.0',
    # unimplemented
    # 'r.1[1,]=r.1[1,]==1.0',

    'b.hex=runif(r3.hex[,1])',
    'b.hex=runif(r3.hex[1,])',

    # 'r.1[,1]=r.1[,1] + 1.3',
    # 'r<n>.hex=min(r.1,1+2)',
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
    "r.1",       #  Simple ref
    # syntax error "missing ')'"
    # This doesn't work anymore?
    # "+(1.23,2.34)",#  prefix 3.57
    ## "+(1.23)",     #  Syntax error, not enuf args
    ## "+(1.23,2,3)", #  Syntax error, too many args
    "r.1[2,3]",  #  Scalar selection
    ## "r.1[2,+]",  #  Function not allowed
    "r.1[2+4,-4]", #  Select row 6, all-cols but 4
    # "r.1[1,-1]; r.1[2,-2]; r.1[3,-3]", #  Partial results are freed
    "r.1[1,-1]; r.1[1,-1]; r.1[1,-1]", #  Partial results are freed
    ## "r.1[2+3,r.1]",#  Error: col selector has too many columns
    # "r.1[2,]",   #  Row 2 all cols
    "r.1[1,]",   #  Row 2 all cols
    # "r.1[,3]",   #  Col 3 all rows
    "r.1[,1]",   #  Col 3 all rows

    "r.1+1",     #  Broadcast scalar over ary
    "r.1-r.1",
    "1.23+(r.1-r.1)",
    "(1.23+r.1)-r.1",
    "min(r.1,1+2)",
    "is.na(r.1)",
    "nrow(r.1)*3",
    "r.1[nrow(r.1)-1,ncol(r.1)-1]",
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
    ## "x=1;x=r.1", #  Allowed to change types via shadowing at REPL level
    "a=r.1",     #  Top-level assignment back to H2O.STORE
    ## "x<-+",
    # ?
    ## "(r.1+1)<-2",
    "r.1[nrow(r.1),]",
    "r.1[,ncol(r.1)]",
    # double semi doesn't work
    # "r.1[2,3]<-4;",
    "c(1,3,5)",
    # doesn't work
    # "function(x){y=1+2;y=c(1,2)}",#  Not allowed to change types in inner scopes
    "sum(1,2,3)",
    "sum(c(1,3,5))",
    "sum(4,c(1,3,5),2,6)",
    "sum(1,r.1,3)",
    # unimplemented?
    "r.1[,c(1)]",
    "r.1[c(1),]",
    "r.1[c(1,3,5),]",
    "r.1[,c(1,3,5)]",
    "a=c(11,22,33,44,55,66); a[c(2,6,1),]",
    #  Filter/selection
    "r.1[r.1[,1]>4,]",
    "apply(r.1,2,sum)",
    # doesn't work
    # "y=5;apply(r.1,1,function(x){x[]+y})",
    # doesn't work
    # "apply(r.1,1,function(x){x=1;r.1})",
    # doesn't work
    # "apply(r.1,1,function(x){r.1})",
    "mean=function(x){apply(x,2,sum)/nrow(x)};mean(r.1)",
    # "mean=function(x){apply(x,1,sum)/nrow(x)};mean(r.1)",

    #  Conditional selection; 
    "ifelse(0,1,2)",
    "ifelse(0,r.1+1,r.1+2)",
    "ifelse(r.1>3,99,r.1)",#  Broadcast selection
    "ifelse(0,+,*)(1,2)",      #  Select functions
    #  Impute the mean
    # doesn't work
    # "apply(r.1,2,function(x){total=sum(ifelse(is.na(x),0,x)); rcnt=nrow(x)-sum(is.na(x)); mean=total / rcnt; ifelse(is.na(x),mean,x)})",
    "factor(r.1[,5])",

    #  Slice assignment & map
    "r.1[,1]",
    "r.1[1,]",
    "r.1[,1]+1",
    "r.1[1,]+1",
    "r.1[,1]=3.3;r.1",  #  Replace a col with a constant
    # unimplemented
    # "r.1[1,]=3.3;r.1",
    "r.1[,1]=r.1[,1]+1",#  Replace a col
    # returns 'unimplemented"
    # "r.1[1,]=r.1[1,]+1",
    "r.1[,ncol(r.1)+1]=4",#  Extend a col
    # unimplemented
    # "r.1[nrow(r.1)+1,]=4",
    "a=ncol(r.1); r.1[,c(a+1,a+2)]=5",#  Extend two cols
    # doesn't work
    # "table(r.1)",
    # doesn't work. wants integer
    # "table(r.1[,1])",


    # "r.1[r.1[,2]>4,]=-99",
    # "r.1[2,]=r.1[7,]",
    # "r.1[c(1,3,5),1] = r.1[c(2,4,6),2]",
    # "r.1[c(1,3,5),1] = r.1[c(2,4),2]",
    # "map()",
    # "map(1)",
    # "map(+,r.1,1)",
    # "map(+,1,2)",
    # "map(function(x){x[];1},r.1)",
    # "map(function(a,b,d){a+b+d},r.1,r.1,1)",
    # "map(function(a,b){a+ncol(b)},r.1,r.1)",

    "a=0;x=0",     #  Delete keys from global scope
    "a=c(1,2,3); a[a[,1]>1,1]",
    "z.hex=a=c(1,2,3); a[a[,1]>1,1]",
    # a problem if it results in the empty set?
    # num rows seems to be zero...h2o gives nans
    # remove for now
    # "a=c(1,2,3); a[a[,1]>10,1]",
    # "z.hex=a=c(1,2,3); a[a[,1]>10,1];",
    ]

    # leave ternary out
    # "(0 ? + : *)(1,2)",        #  Trinary select
    # "(1? r.1 : (r.1+1))[1,2]",#  True (vs false) test

if DO_FUNCTION:
    exprListFull += [
        # what is this?
        ### "function(=){x+1}(2)",
        # doesn't work?
        # "function(x,=){x+1}(2)",
        # doesn't work
        # "function(x,<-){x+1}(2)",
        # doesn't work
        # "function(x,x){x+1}(2)",
        "function(x,y,z){x[]}(r.1,1,2)",
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
        # "function(a){a[];a=1}",
        "a=1;a=2;function(x){x=a;a=3}",
        "a=r.1;function(x){x=a;a=3;nrow(x)*a}(a)",
        #  Higher-order function typing: fun is typed in the body of function(x)
        "function(funy){function(x){funy(x)*funy(x)}}(sgn)(-2)",
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
        global SEED, localhost
        SEED = h2o.setup_random_seed()
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(1, java_heap_GB=2)
        else:
            h2o_hosts.build_cloud_with_hosts(1)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_exec2_operators(self):
        h2o.beta_features = True
        bucket = 'home-0xdiag-datasets'
        # csvPathname = 'airlines/year2013.csv'
        csvPathname = 'standard/covtype.data'
        hexKey = 'i.hex'
        parseResult = h2i.import_parse(bucket=bucket, path=csvPathname, schema='put', hex_key=hexKey)

        for resultKey, execExpr in initList:
            h2e.exec_expr(h2o.nodes[0], execExpr, resultKey=None, timeoutSecs=10)
        start = time.time()
        # h2e.exec_expr_list_rand(len(h2o.nodes), exprList, 'r.1', maxTrials=200, timeoutSecs=10)
        # h2e.exec_expr_list_rand(len(h2o.nodes), exprList, None, maxTrials=200, timeoutSecs=30)
        for execExpr in exprList:
            h2e.exec_expr(h2o.nodes[0], execExpr, resultKey=None, timeoutSecs=30)

        h2o.check_sandbox_for_errors()
        print "exec end on ", "operators" , 'took', time.time() - start, 'seconds'


if __name__ == '__main__':
    h2o.unit_main()
