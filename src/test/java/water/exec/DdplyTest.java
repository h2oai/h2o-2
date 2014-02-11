package water.exec;

import static org.junit.Assert.fail;

import org.junit.BeforeClass;
import org.junit.Test;
import water.Key;
import water.Lockable;
import water.TestUtil;
import water.fvec.Frame;

public class DdplyTest extends TestUtil {
  @BeforeClass public static void stall() { stall_till_cloudsize(1); }

  // This test is intended to use a file large enough to strip across multiple
  // nodes with multiple groups, to test that all generated groups are both
  // built and executed distributed.
  /*@Test*/ public void testDdplyBig() {
    Key k1 = Key.make("orange.hex");
    Key k2 = Key.make("covtype.hex");
    try {
      // A big enough file to distribute across multiple nodes.
      Frame fr1 = parseFrame(k1,"smalldata/unbalanced/orange_small_train.data.zip");
      System.out.println(fr1);
      checkStr("ddply(orange.hex,c(7),sum)");

      Frame fr2 = parseFrame(k2,"../datasets/UCI/UCI-large/covtype/covtype.data");
      System.out.println(fr2);
      checkStr("ddply(covtype.hex,c(11),sum)");

    } finally {
      Lockable.delete(k1);    // Remove original hex frame key
      Lockable.delete(k2);    // Remove original hex frame key
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
        fail("Not a Frame result");
      }
    } 
    catch( IllegalArgumentException iae ) { fail(iae.getMessage()); }
    if( env != null ) env.remove_and_unlock();
  }
}
