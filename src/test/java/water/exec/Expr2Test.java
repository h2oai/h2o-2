package water.exec;

import org.junit.Test;
import java.io.File;
import water.*;
import water.fvec.*;

public class Expr2Test extends TestUtil {
  int i = 0;

  /*@Test*/ public void testBasicExpr1() {
    Key dest = Key.make("h.hex");
    try {
      File file = TestUtil.find_test_file("smalldata/iris/iris_wheader.csv");
      //File file = TestUtil.find_test_file("smalldata/cars.csv");
      Key fkey = NFSFileVec.make(file);
      ParseDataset2.parse(dest,new Key[]{fkey});
      UKV.remove(fkey);

      checkStr("1.23");         // 1.23
      checkStr(" 1.23 + 2.34"); // 3.57
      checkStr(" 1.23 + 2.34 * 3"); // 10.71, L2R eval order
      checkStr(" 1.23 2.34");   // Syntax error
      checkStr("1.23 < 2.34");  // 1
      checkStr("1.23 <=2.34");  // 1
      checkStr("1.23 > 2.34");  // 0
      checkStr("1.23 >=2.34");  // 0
      checkStr("1.23 ==2.34");  // 0
      checkStr("1.23 !=2.34");  // 1
      checkStr("h.hex");        // Simple ref
      checkStr("+(1.23,2.34)"); // prefix 3.57
      checkStr("+(1.23)");      // Syntax error, not enuf args
      checkStr("+(1.23,2,3)");  // Syntax error, too many args
      checkStr("h.hex[2,3]");   // Scalar selection
      checkStr("h.hex[2,+]");   // Function not allowed
      checkStr("h.hex[2+4,-4]");// Select row 6, all-cols but 4
      checkStr("h.hex[1,-1]; h.hex[2,-2]; h.hex[3,-3]");// Partial results are freed
      checkStr("h.hex[2+3,h.hex]"); // Error: col selector has too many columns
      checkStr("h.hex[2,]");    // Row 2 all cols
      checkStr("h.hex[,3]");    // Col 3 all rows
      checkStr("h.hex+1");      // Broadcast scalar over ary
      checkStr("h.hex-h.hex");
      checkStr("1.23+(h.hex-h.hex)");
      checkStr("(1.23+h.hex)-h.hex");
      checkStr("min(h.hex,1+2)");
      checkStr("is.na(h.hex)");
      checkStr("nrow(h.hex)*3");
      checkStr("h.hex[ncol(h.hex),nrow(h.hex)]");
      checkStr("1=2");
      checkStr("x");
      checkStr("x+2");
      checkStr("2+x");
      checkStr("x=1");
      checkStr("x<-1");
      checkStr("x=1;x=h.hex");  // Allowed to change types via shadowing at REPL level
      checkStr("a=h.hex");      // Top-level assignment back to H2O.STORE
      checkStr("x<-+");
      checkStr("(h.hex+1)<-2");
      checkStr("h.hex[nrow(h.hex=1),]");
      checkStr("h.hex[2,3]<-4;");
      checkStr("c(1,3,5)");
      checkStr("function(=){x+1}(2)");
      checkStr("function(x,=){x+1}(2)");
      checkStr("function(x,<-){x+1}(2)");
      checkStr("function(x,x){x+1}(2)");
      checkStr("function(x,y,z){x[]}(h.hex,1,2)");
      checkStr("function(x){x[]}(2)");
      checkStr("function(x){x+1}(2)");
      checkStr("function(x){y=x+y}(2)");
      checkStr("function(x){}(2)");
      checkStr("function(x){y=x*2; y+1}(2)");
      checkStr("function(x){y=1+2}(2)");
      checkStr("function(x){y=1+2;y=c(1,2)}"); // Not allowed to change types in inner scopes
      checkStr("sum(1,2,3)");
      checkStr("sum(c(1,3,5))");
      checkStr("sum(4,c(1,3,5),2,6)");
      checkStr("sum(1,h.hex,3)");
      checkStr("h.hex[,c(1,3,5)]");
      checkStr("h.hex[c(1,3,5),]");
      checkStr("function(a){a[];a=1}");
      checkStr("a=1;a=2;function(x){x=a;a=3}");
      checkStr("a=h.hex;function(x){x=a;a=3;nrow(x)*a}(a)");
      // Higher-order function typing: fun is typed in the body of function(x)
      checkStr("function(funy){function(x){funy(x)*funy(x)}}(sgn)(-2)");
      // Filter/selection
      checkStr("h.hex[h.hex[,2]>4,]");
      checkStr("a=c(1,2,3);a[a[,1]>10,1]");
      checkStr("apply(h.hex,2,sum)");
      checkStr("y=5;apply(h.hex,2,function(x){x[]+y})");
      checkStr("apply(h.hex,2,function(x){x=1;h.hex})");
      checkStr("apply(h.hex,2,function(x){h.hex})");
      checkStr("mean=function(x){apply(x,2,sum)/nrow(x)};mean(h.hex)");
      
      // Conditional selection; 
      checkStr("ifelse(0,1,2)");
      checkStr("ifelse(0,h.hex+1,h.hex+2)");
      checkStr("ifelse(h.hex>3,99,h.hex)"); // Broadcast selection
      checkStr("ifelse(0,+,*)(1,2)");       // Select functions
      checkStr("(0 ? + : *)(1,2)");         // Trinary select
      checkStr("(1? h.hex : (h.hex+1))[1,2]"); // True (vs false) test
      // Impute the mean
      checkStr("apply(h.hex,2,function(x){total=sum(ifelse(is.na(x),0,x)); rcnt=nrow(x)-sum(is.na(x)); mean=total / rcnt; ifelse(is.na(x),mean,x)})");

      // Slice assignment & map
      //checkStr("h.hex[h.hex[,2]>4,]=-99");
      //checkStr("h.hex[2,]=h.hex[7,]");
      //checkStr("h.hex[,2]=h.hex[,7]+1");
      //checkStr("h.hex[c(1,3,5),1] = h.hex[c(2,4,6),2]");
      //checkStr("h.hex[c(1,3,5),1] = h.hex[c(2,4),2]");
      //checkStr("map()");
      //checkStr("map(1)");
      //checkStr("map(+,h.hex,1)");
      //checkStr("map(+,1,2)");
      //checkStr("map(function(x){x[];1},h.hex)");
      //checkStr("map(function(a,b,d){a+b+d},h.hex,h.hex,1)");
      //checkStr("map(function(a,b){a+ncol(b)},h.hex,h.hex)");

      // Needed examples: 
      // (2) Drop 95% outliers (top & bot 2.5% outliers)
      // (3) DONE Table command? (co-occurance matrix)
      // (4) Cut command?
      checkStr("a=0;x=0");      // Delete keys from global scope

    } finally {
      UKV.remove(dest);         // Remove original hex frame key
    }
  }

  void checkStr( String s ) {
    Env env=null;
    try { 
      System.out.println(s);
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
    if( env != null ) env.remove();
  }

}
