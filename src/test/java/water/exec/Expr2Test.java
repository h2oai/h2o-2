package water.exec;

import static org.junit.Assert.*;
import org.junit.Test;
import water.exec.*;
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
      Frame fr = ParseDataset2.parse(dest,new Key[]{fkey});
      UKV.remove(fkey);

      checkStr("1.23");
      checkStr(" 1.23 + 2.34");
      checkStr(" 1.23 2.34");
      checkStr("h.hex");
      checkStr("h.hex+1");
      checkStr("h.hex-h.hex");
      checkStr("1.23+(h.hex-h.hex)");
      checkStr("(1.23+h.hex)-h.hex");

    } finally {
      UKV.remove(dest);         // Remove original hex frame key
    }
  }

  void checkStr( String s ) {
    Frame res=null;
    try { res = Exec2.exec(s); } 
    catch( IllegalArgumentException iae ) { System.out.println(iae.getMessage()); }
    if( res != null ) res.remove();
  }

}
