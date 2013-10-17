package water.exec;

import org.junit.Test;
import java.io.File;
import water.*;
import water.fvec.*;

public class Expr2Test extends TestUtil {
  int i = 0;

  @Test public void testBasicExpr1() {
    Key dest = Key.make("h.hex");
    try {
      File file = TestUtil.find_test_file("smalldata/cars.csv");
      Key fkey = NFSFileVec.make(file);
      ParseDataset2.parse(dest,new Key[]{fkey});
      UKV.remove(fkey);

      checkStr("1.23");         // 1.23
      checkStr(" 1.23 + 2.34"); // 3.57
      checkStr(" 1.23 2.34");   // Syntax error
      checkStr("h.hex");        // Simple ref
      checkStr("+(1.23,2.34)"); // prefix 3.57
      checkStr("+(1.23)");      // Syntax error, not enuf args
      checkStr("+(1.23,2,3)");  // Syntax error, too many args
      checkStr("h.hex[2,3]");   // Scalar selection
      checkStr("h.hex[2+3,-4*5]");
      checkStr("h.hex[2+3,h.hex]");
      checkStr("h.hex[2,]");
      checkStr("h.hex[,3]");
      checkStr("h.hex+1");
      checkStr("h.hex-h.hex");
      checkStr("1.23+(h.hex-h.hex)");
      checkStr("(1.23+h.hex)-h.hex");
      checkStr("min(h.hex,1+2)");
      checkStr("isNA(h.hex)");
      checkStr("h.hex[ncol(h.hex),nrow(h.hex)]");
      checkStr("1=2");
      checkStr("(h.hex+1)=2");
      checkStr("h.hex[nrow(h.hex=1),]");
      checkStr("h.hex[2,3]=4");
      checkStr("h.hex[2,]=h.hex[7,]");
      checkStr("h.hex[,2]=h.hex[,7]+1");
      checkStr("c(1,3,5)");
      checkStr("h.hex[c(1,3,5),]");
      checkStr("h.hex[c(1,3,5),1] = h.hex[c(2,4,6),2]");
      checkStr("h.hex[c(1,3,5),1] = h.hex[c(2,4),2]");
      checkStr("function(=){x+1}(2)");
      checkStr("function(x,=){x+1}(2)");
      checkStr("function(x,x){x+1}(2)");
      checkStr("function(x){x[]}(h.hex)");
      checkStr("function(x){x[]}(2)");
      checkStr("function(x){x+1}(2)");

      // Needed examples: 
      // (1) Replace NAs with imputed mean
      // (2) Drop 95% outliers (top & bot 2.5% outliers)


    } finally {
      UKV.remove(dest);         // Remove original hex frame key
    }
  }

  void checkStr( String s ) {
    Env env=null;
    try { 
      env = Exec2.exec(s); 
      System.out.println(env);
    } 
    catch( IllegalArgumentException iae ) { System.out.println(iae.getMessage()); }
    if( env != null ) env.remove();
  }

}
