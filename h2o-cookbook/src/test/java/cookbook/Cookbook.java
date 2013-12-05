package cookbook;

import org.junit.*;
import water.*;
import water.fvec.*;
import water.util.Log;
import water.util.RemoveAllKeysTask;

public class Cookbook extends TestUtil {
  @Before
  public void removeAllKeys() {
    Log.info("Removing all keys...");
    RemoveAllKeysTask collector = new RemoveAllKeysTask();
    collector.invokeOnAllNodes();
    Log.info("Removed all keys.");
  }

//  @Test
//  public void testWillFail() {
//    throw new RuntimeException("first test fails");
//  }

  // ---
  // Test flow-coding a filter & group-by computing e.g. mean
  @Test
  public void testBasic() {
    Key k = Key.make("cars.hex");
    Frame fr = parseFrame(k, "../smalldata/cars.csv");
    //Frame fr = parseFrame(k, "../datasets/UCI/UCI-large/covtype/covtype.data");
    // Call into another class so we do not need to weave anything in this class
    // when run as a JUnit
    Cookbook2.basicStatic(k, fr);
  }

//  @Test
//  public void testWillFail2() {
//    throw new RuntimeException("3 test fails");
//  }
}
