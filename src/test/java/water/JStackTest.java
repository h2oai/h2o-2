package water;

import water.api.JStack;
import org.junit.*;

public class JStackTest extends TestUtil {

  @BeforeClass public static void stall() { stall_till_cloudsize(3); }

  @Test public void testJStack() {
    for( int i=0; i<10; i++ ) {
      JStack js = new JStack();
      js.serve();
      Assert.assertEquals(js.nodes.length,H2O.CLOUD.size());
    }
  }
}
