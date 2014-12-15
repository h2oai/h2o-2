package water.exec;

import hex.FrameSplitter;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import water.H2O;
import water.Key;
import water.Lockable;
import water.TestUtil;
import water.fvec.Frame;
import water.fvec.NFSFileVec;
import water.fvec.ParseDataset2;

import java.io.File;

import static org.junit.Assert.*;

public class Expr2Test extends TestUtil {
  @BeforeClass public static void stall() { stall_till_cloudsize(1); }

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test public void rbindTest() {
    Key dest1 = Key.make("f1");
    float[] ratios  = arf(0.5f);
    Frame[] splits  = null;

    File file1 = TestUtil.find_test_file("smalldata/tnc3_10.csv");
    //File file = TestUtil.find_test_file("smalldata/iris/iris_wheader.csv");
    //File file = TestUtil.find_test_file("smalldata/cars.csv");
    Key fkey1 = NFSFileVec.make(file1);
    Frame f = ParseDataset2.parse(dest1,new Key[]{fkey1});
    FrameSplitter fs = new FrameSplitter(f, ratios);
    H2O.submitTask(fs).join();
    splits = fs.getResult();

    Frame rbinded_frame;
    Env ev = Exec2.exec("rbind("+splits[0]._key+","+splits[1]._key+")" );
    try {
      rbinded_frame = ev.popAry();
    } finally {
      if (ev!=null) ev.remove_and_unlock();
    }
    assertEquals(rbinded_frame.numRows(),f.numRows());

    rbinded_frame.delete();
    Lockable.delete(dest1);
    for (Frame s : splits)
      if (s != null) s.delete();
  }

  @Test public void testBasicExpr1() {
    Key dest = Key.make("h.hex");
    try {
      File file = TestUtil.find_test_file("smalldata/tnc3_10.csv");
      //File file = TestUtil.find_test_file("smalldata/iris/iris_wheader.csv");
      //File file = TestUtil.find_test_file("smalldata/cars.csv");
      Key fkey = NFSFileVec.make(file);
      ParseDataset2.parse(dest,new Key[]{fkey});


      // Simple numbers & simple expressions
      checkStr("1.23",1.23);
      checkStr(" 1.23 + 2.34",3.57);
      checkStr(" 1.23 + 2.34 * 3", 8.25); // op precedence of * over +
      checkStr(" 1.23 2.34", "Junk at end of line\n"+" 1.23 2.34\n"+"      ^--^\n");   // Syntax error
      checkStr("1.23 < 2.34",1);
      checkStr("1.23 <=2.34",1);
      checkStr("1.23 > 2.34",0);
      checkStr("1.23 >=2.34",0);
      checkStr("1.23 ==2.34",0);
      checkStr("1.23 !=2.34",1);
      checkStr("1 & 2",1);
      checkStr("NA&0",0);       // R-spec: 0 not NA
      checkStr("0&NA",0);       // R-spec: 0 not NA
      checkStr("NA&1",Double.NaN); // R-spec: NA not 1
      checkStr("1&NA",Double.NaN);
      checkStr("1|NA",1);
      checkStr("1&&2",1);
      checkStr("1||0",1);
      checkStr("NA||1",1);
      checkStr("NA||0",Double.NaN);
      checkStr("0||NA",Double.NaN);
      checkStr("!1",0);
      checkStr("(!)(1)",0);
      checkStr("(!!)(1)", "Arg 'x' typed as dblary but passed dblary(dblary)\n"+"(!!)(1)\n"+" ^-^\n");
      checkStr("-1",-1);
      checkStr("-(1)",-1);
      checkStr("(-)(1)", "Passed 1 args but expected 2\n"+"(-)(1)\n"+"   ^--^\n");
      checkStr("-T",-1);
      checkStr("* + 1", "Arg 'x' typed as dblary but passed anyary{dblary,dblary,}(dblary,dblary)\n"+"* + 1\n"+"^----^\n");
      // Simple op as prefix calls
      checkStr("+(1.23,2.34)","Missing ')'\n"+"+(1.23,2.34)\n"+"  ^---^\n"); // Syntax error: looks like unary op application
      checkStr("+(1.23)",1.23); // Unary operator

      // Simple scalar assignment
      checkStr("1=2","Junk at end of line\n"+"1=2\n"+" ^^\n");
      checkStr("x","Unknown var x\n"+"x\n"+"^^\n");
      checkStr("x+2","Unknown var x\n"+"x+2\n"+"^^\n");
      checkStr("2+x","Missing expr or unknown ID\n"+"2+x\n"+"  ^\n");
      checkStr("x=1",1);
      checkStr("x<-1",1);       // Alternative R assignment syntax
      checkStr("x=3;y=4",4);    // Return value is last expr

      // Ambiguity & Language
      checkStr("x=mean");         // Assign x to the built-in fcn mean
      checkStr("x=mean=3",3);     // Assign x & id mean with 3; "mean" here is not related to any built-in fcn
      checkStr("x=mean(c(3))",3); // Assign x to the result of running fcn mean(3)
      checkStr("x=mean(c(\n3))",3); // Assign x to the result of running fcn mean(3)
      checkStr("x=mean+3","Arg 'x' typed as dblary but passed dbl(ary)\n"+"x=mean+3\n"+"  ^-----^\n");       // Error: "mean" is a function; cannot add a function and a number
      checkStr("apply(c(1,2,3),,nrow)","Missing argument\napply(c(1,2,3),,nrow)\n               ^\n");
      checkStr("foo==bar","Unknown var foo\nfoo==bar\n^--^\n"); // Error msg is about "foo==" and not new assignment "foo="

      // Simple array handling; broadcast operators
      checkStr("h.hex");        // Simple ref
      checkStr("sum(apply(h.hex[,c(4,5)],1,mean))",183.96); // Row-wise apply on mean
      checkStr("h.hex[2,3]",1); // Scalar selection
      checkStr("h.hex[2,+]","Must be scalar or array\n"+"h.hex[2,+]\n"+"        ^-^\n");   // Function not allowed
      checkStr("h.hex[2+4,-4]");// Select row 6, all-cols but 4
      checkStr("h.hex[1,-1]; h.hex[2,-2]; h.hex[3,-3]");// Partial results are freed
      checkStr("h.hex[2+3,h.hex]","Selector must be a single column: [pclass, name, sex, age, sibsp, parch, ticket, fare, cabin, embarked, boat, body, home.dest, survived]"); // Error: col selector has too many columns
      checkStr("h.hex[2,]");    // Row 2 all cols
      checkStr("h.hex[,3]");    // Col 3 all rows
      checkStr("h.hex+1");      // Broadcast scalar over ary
      checkStr("h.hex-h.hex");
      checkStr("1.23+(h.hex-h.hex)");
      checkStr("(1.23+h.hex)-h.hex");
      checkStr("min(h.hex,1+2)",0);
      checkStr("max(h.hex,1+2)",211.3375);
      checkStr("min.na.rm(h.hex,NA)",0); // 0
      checkStr("max.na.rm(h.hex,NA)",211.3375); // 211.3375
      checkStr("min.na.rm(c(NA, 1), -1)",-1); // -1
      checkStr("max.na.rm(c(NA, 1), -1)", 1); // 1
      checkStr("max(c(Inf,1),  2 )", Double.POSITIVE_INFINITY); // Infinity
      checkStr("min(c(Inf,1),-Inf)", Double.NEGATIVE_INFINITY); // -Infinity
      checkStr("is.na(h.hex)");
      checkStr("sum(is.na(h.hex))", 0);
      checkStr("nrow(h.hex)*3", 30);
      checkStr("h.hex[nrow(h.hex)-1,ncol(h.hex)-1]");
      checkStr("x=1;x=h.hex");  // Allowed to change types via shadowing at REPL level
      checkStr("a=h.hex");      // Top-level assignment back to H2O.STORE

      checkStr("(h.hex+1)<-2","Junk at end of line\n"+"(h.hex+1)<-2\n"+"         ^-^\n"); // No L-value
      checkStr("h.hex[nrow(h.hex=1),]","Arg 'x' typed as ary but passed dbl\n"+"h.hex[nrow(h.hex=1),]\n"+"          ^--------^\n"); // Passing a scalar 1.0 to nrow
      checkStr("h.hex[{h.hex=10},]"); // ERROR BROKEN: SHOULD PARSE statement list here; then do evil side-effect killing h.hex but also using 10 to select last row
      checkStr("h.hex[3,4]<-4;",4);
      checkStr("c(1,3,5)");
      // Column row subselection
      checkStr("h.hex[,c(1,3,5)]");
      checkStr("h.hex[c(1,3,5),]");
      checkStr("a=c(11,22,33,44,55,66); a[c(2,6,1),]");
      // Named column selection
      checkStr("h.hex$ 2","Missing column name after $\nh.hex$ 2\n      ^^\n");
      checkStr("h.hex$crunk","Missing column crunk in frame [pclass, name, sex, age, sibsp, parch, ticket, fare, cabin, embarked, boat, body, home.dest, survived]");
      checkStr("h.hex$pclass");
      checkStr("mean(h.hex$pclass)",1);

      // More complicated operator precedence
      checkStr("c(1,0)&c(2,3)");// 1,0
      checkStr("c(2,NA)&&T",1); // 1
      checkStr("-(x = 3)",-3);
      checkStr("x<-+");
      checkStr("x<-+;x(2)","Passed 1 args but expected 2\nx<-+;x(2)\n      ^--^\n"); // Error, + is binary if used as prefix
      checkStr("x<-+;x(1,2)",3); // 3
      checkStr("x<-*;x(2,3)",6); // 6
      checkStr("x=c(0,1);!x+1"); // ! has lower precedence
      checkStr("x=c(1,-2);-+---x");
      checkStr("x=c(1,-2);--!--x");
      checkStr("!(y=c(3,4))");
      checkStr("!x!=1");
      checkStr("(!x)!=1");
      checkStr("1+x^2");
      checkStr("1+x**2");
      checkStr("x + 2/y");
      checkStr("x + (2/y)");
      checkStr("-x + y");
      checkStr("-(x + y)");
      checkStr("-x % y");
      checkStr("-(x % y)");
      checkStr("T|F&F",1);      // Evals as T|(F&F)==1 not as (T|F)&F==0
      checkStr("T||F&&F",1);    // Evals as T|(F&F)==1 not as (T|F)&F==0

      // User functions
      checkStr("function(=){x+1}(2)","Invalid var\nfunction(=){x+1}(2)\n         ^\n");
      checkStr("function(x,=){x+1}(2)","Invalid var\nfunction(x,=){x+1}(2)\n           ^\n");
      checkStr("function(x,<-){x+1}(2)","Invalid var\nfunction(x,<-){x+1}(2)\n           ^\n");
      checkStr("function(x,x){x+1}(2)","Repeated argument\nfunction(x,x){x+1}(2)\n           ^^\n");
      checkStr("function(x,y,z){x[]}(h.hex,1,2)");
      checkStr("function(x){x[]}(2)","Arg 'x' typed as ary but passed dbl\nfunction(x){x[]}(2)\n                ^--^\n");
      checkStr("function(x){x+1}(2)",3);
      checkStr("function(x){y=x+y}(2)");
      checkStr("function(x){}(2)");
      checkStr("function(x){y=x*2; y+1}(2)",5);
      checkStr("function(x){y=1+2}(2)",3);
      checkStr("function(x){y=1+2;y=c(1,2)}"); // Not allowed to change types in inner scopes
      checkStr("a=function(x) x+1; 7",7); // Function def w/out curly-braces; return 7
      checkStr("a=function(x) {x+1}; 7",7); // Function def w/ curly-braces; return 7
      checkStr("a=function(x) {x+1; 7}"); // Function def of 7
      checkStr("c(1,c(2,3))");
      checkStr("a=c(1,Inf);c(2,a)");
      // Test sum flattening all args
      checkStr("sum(1,2,3)",6);
      checkStr("sum(c(1,3,5))",9);
      checkStr("sum(4,c(1,3,5),2,6)",21);
      checkStr("sum(1,h.hex,3)"); // should report an error because h.hex has enums
      checkStr("sum(c(NA,-1,1))",Double.NaN);
      checkStr("sum.na.rm(c(NA,-1,1))",0);

      checkStr("function(a){a[];a=1}");
      checkStr("a=1;a=2;function(x){x=a;a=3}");
      checkStr("a=h.hex;function(x){x=a;a=3;nrow(x)*a}(a)",30);
      checkStr("a=h.hex;a[,1]=(a[,1]==8)");
      // Higher-order function typing: fun is typed in the body of function(x)
      checkStr("function(funy){function(x){funy(x)*funy(x)}}(sgn)(-2)",1);
      // Filter/selection
      checkStr("h.hex[h.hex[,4]>30,]");
      checkStr("a=c(1,2,3);a[a[,1]>10,1]");
      checkStr("sapply(a,sum)[1,1]",6);
      checkStr("apply(h.hex,2,sum)"); // ERROR BROKEN: the ENUM cols should fold to NA
      checkStr("y=5;apply(h.hex,2,function(x){x[]+y})");
      checkStr("apply(h.hex,2,function(x){x=1;h.hex})","Arg 'fcn' typed as ary(ary) but passed ary(dbl)\napply(h.hex,2,function(x){x=1;h.hex})\n     ^-------------------------------^\n");
      checkStr("apply(h.hex,2,function(x){h.hex})","apply requires that ary fun(ary x) return 1 column");
      checkStr("apply(h.hex,2,function(x){sum(x)/nrow(x)})");
      checkStr("mean=function(x){apply(x,2,sum)/nrow(x)};mean(h.hex)");

      // Conditional selection;
      checkStr("ifelse(0,1,2)",2);
      checkStr("ifelse(0,h.hex+1,h.hex+2)");
      checkStr("ifelse(h.hex>3,99,h.hex)"); // Broadcast selection
      checkStr("ifelse(0,+,*)(1,2)",2);     // Select functions
      checkStr("(0 ? + : *)(1,2)",2);       // Trinary select
      checkStr("(1? h.hex : (h.hex+1))[1,2]",0); // True (vs false) test
      // Impute the mean
      checkStr("apply(h.hex,2,function(x){total=sum(ifelse(is.na(x),0,x)); rcnt=nrow(x)-sum(is.na(x)); mean=total / rcnt; ifelse(is.na(x),mean,x)})");
      checkStr("factor(h.hex[,5])");

      // Slice assignment & map
      checkStr("h.hex[,2]");
      checkStr("h.hex[,2]+1");
      checkStr("h.hex[,3]=3.3;h.hex");   // Replace a col with a constant
      checkStr("h.hex[,3]=h.hex[,2]+1"); // Replace a col
      checkStr("h.hex[,ncol(h.hex)+1]=4"); // Extend a col
      checkStr("a=ncol(h.hex);h.hex[,c(a+1,a+2)]=5"); // Extend two cols
      checkStr("h.hex[,7]=x=3; !(x+2)");
      checkStr("table(h.hex)");
      checkStr("table(h.hex[,5])");
      checkStr("table(h.hex[,c(2,7)])");
      checkStr("table(h.hex[,c(2,9)])");
      checkStr("a=cbind(c(1,2,3), c(4,5,6))");
      checkStr("a[,1] = factor(a[,1])");
      checkStr("is.factor(a[,1])",1);
      checkStr("isTRUE(c(1,3))",0);
      checkStr("a=1;isTRUE(1)",1);
      checkStr("a=c(1,2);isTRUE(a)",0);
      checkStr("isTRUE(min)",0);
      checkStr("seq_len(0)","Error in seq_len(0): argument must be coercible to positive integer");
      checkStr("seq_len(-1)","Error in seq_len(-1): argument must be coercible to positive integer");
      checkStr("seq_len(10)");
      checkStr("3 < 4 |  F &  3 > 4", 1); // Evals as (3<4) | (F & (3>4))
      checkStr("3 < 4 || F && 3 > 4", 1);
      checkStr("h.hex[,4] != 29 || h.hex[,2] < 305 && h.hex[,2] < 81", Double.NaN);
      //checkStr("h.hex[h.hex[,4]>40,]=-99");
      //checkStr("h.hex[2,]=h.hex[7,]");
      //checkStr("h.hex[c(1,3,5),1] = h.hex[c(2,4,6),2]");
      //checkStr("h.hex[c(1,3,5),1] = h.hex[c(2,4),2]");
      //checkStr("map()");
      //checkStr("map(1)");
      //checkStr("map(+,h.hex,1)");
      //checkStr("map(+,1,2)");
      //checkStr("map(function(x){x[];1},h.hex)");
      //checkStr("map(function(a,b,d){a+b+d},h.hex,h.hex,1)");
      //checkStr("map(function(a,b){a+ncol(b)},h.hex,h.hex)");

      // Quantile
      checkStr("quantile(seq_len(10),seq_len(10)/10)");
      checkStr("quantile(runif(seq_len(10000),-1),seq_len(10)/10)");
      checkStr("quantile(h.hex[,4],c(0,.05,0.3,0.55,0.7,0.95,0.99))");

      // ddply error checks
      checkStr("ddply(h.hex,h.hex,sum)","Only one column-of-columns for column selection");
      checkStr("ddply(h.hex,seq_len(10000),sum)","Too many columns selected");
      checkStr("ddply(h.hex,NA,sum)","NA not a valid column");
      checkStr("ddply(h.hex,c(1,NA,3),sum)","NA not a valid column");
      checkStr("ddply(h.hex,c(1,99,3),sum)","Column 99 out of range for frame columns 17");

      checkStr("nrow(unique(h.hex[,5]))",3);
      checkStr("nrow(unique(h.hex[,6]))",2);
      checkStr("nrow(unique(h.hex[,c(5,6)]))",4); // multi-column unique

      // Newlines as statement-ends
      checkStr("3*4+5*6",42);
      checkStr("(h.hex[1,1]=2)",2);
      checkStr("(h.hex[1,1]=2\n)",2);
      checkStr("(h.hex[1,1]\n=2)",2);
      checkStr("(h.hex\n[1,1]=2)",2);
      checkStr("function(){x=1.23;(x=4.5)\n}()",4.5);
      checkStr("function(){x=1.23;x=\n4.5\n}()",4.5);
      checkStr("x=3\nfunction()x=1.23\nx",3);
      checkStr("x=3\nfunction(){(x=1.23)}\nx",3);
      checkStr("x=function(df)\n{\nmin(df$age)\n}\n;x(h.hex)",0.92);
      checkStr("1.23\n-4",-4);
      checkStr("1.23 +\n-4",-2.77);
      checkStr("x=3;3*-x",-9);  // *- is not a token
      checkStr("x=3;3\n*\n-\nx",3); // Each of '3' and '*' and '-' and 'x' is a standalone statement

      // No strings, yet
      checkStr("function(df) { min(df[,\"age\"]) }","The current Exec does not handle strings\nfunction(df) { min(df[,\"age\"]) }\n                       ^-----^\n");

      // Cleanup testing temps
      checkStr("a=0;x=0;y=0",0); // Delete keys from global scope

    } finally {
      Lockable.delete(dest);    // Remove original hex frame key
    }
  }

  void checkStr( String s ) {
    Env env=null;
    try {
      env = Exec2.exec(s);
      if( env.isAry() ) {       // Print complete frames for inspection
        Frame res = env.popAry();
        String skey = env.key();
        System.out.println(res.toStringAll());
        env.subRef(res,skey);   // But then end lifetime
      } else {
        System.out.println( env.resultString() );
      }
    }
    catch( IllegalArgumentException iae ) { System.out.println(iae.getMessage()); }
    if( env != null ) env.remove_and_unlock();
    debug_print(s);
  }

  void checkStr( String s, double d ) {
    Env env = Exec2.exec(s);
    assertFalse("Should be scalar result not Frame: "+s,env.isAry() );
    assertFalse( env.isFcn() );
    double res = env.popDbl();
    assertEquals(d, res, d / 1e8);
    env.remove_and_unlock();
    debug_print(s);
  }

  void checkStr( String s, String err ) {
    Env env = null;
    try {
      env = Exec2.exec(s);
      env.remove_and_unlock();
      fail(); // Supposed to throw; reaching here is an error
    } catch ( IllegalArgumentException e ) {
      assertEquals(err, e.getMessage());
    }
    debug_print(s);
  }

  // Handy code to debug leaking keys
  public static void debug_print( String s ) {
  //  int sz=0;
  //  int vgs=0, frs=0, vcs=0, cks=0;
  //  for( Key k : H2O.keySet() ) {
  //    sz++;
  //    Value val = DKV.get(k);
  //    Iced ice = TypeMap.newInstance(val.type());
  //    if( ice instanceof Vec.VectorGroup ) vgs++;
  //    else if( ice instanceof Vec ) vcs++;
  //    else if( ice instanceof Chunk ) cks++;
  //    else if( ice instanceof Frame ) frs++;
  //  }
  //  System.out.println("KKK="+(sz-vgs-frs-vcs-cks)+
  //                     ", VGS="+vgs+
  //                     ", FRS="+frs+
  //                     ", VCS="+vcs+
  //                     ", CKS="+cks+
  //                     ", "+s);
  }
}
