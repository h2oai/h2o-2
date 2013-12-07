package water;

import static org.junit.Assert.*;
import org.junit.*;
import water.fvec.*;
import water.exec.Flow;
import water.util.Utils.*;

public class FlowTest extends TestUtil {

  @BeforeClass public static void stall() { stall_till_cloudsize(1); }

  // ---
  // Test flow-coding a filter & group-by computing e.g. mean
  @Test public void testBasic() { 
    Key k = Key.make("cars.hex");
    Frame fr = parseFrame(k, "smalldata/cars.csv");
    //Frame fr = parseFrame(k, "../datasets/UCI/UCI-large/covtype/covtype.data");
    // Call into another class so we do not need to weave anything in this class
    // when run as a JUnit
    FlowTest2.basicStatic(k,fr);
  }
}
