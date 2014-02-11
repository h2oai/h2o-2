package water.exec;

import static org.junit.Assert.fail;

import org.junit.BeforeClass;
import org.junit.Test;
import water.Key;
import water.Lockable;
import water.TestUtil;
import water.fvec.Frame;

public class DdplyTest extends TestUtil {
  @BeforeClass public static void stall() { stall_till_cloudsize(2); }

  // This test is intended to use a file large enough to strip across multiple
  // nodes with multiple groups, to test that all generated groups are both
  // built and executed distributed.
  /*@Test*/ public void testDdplyBig() {
    Key dest = Key.make("orange.hex");
    try {
      // A big enough file to distribute across multiple nodes.
      Frame fr = parseFrame(dest,"smalldata/unbalanced/orange_small_train.data.zip");
      System.out.println(fr);

      checkStr("ddply(orange.hex,c(7),sum)");

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
        fail("Not a Frame result");
      }
    } 
    catch( IllegalArgumentException iae ) { fail(iae.getMessage()); }
    if( env != null ) env.remove_and_unlock();
  }
}
