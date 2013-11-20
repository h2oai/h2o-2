package hex;

import org.junit.Test;
import water.H2O;
import water.Key;
import water.fvec.Chunk;
import water.fvec.Frame;
import water.TestUtil;
import water.fvec.NewChunk;

import static water.TestUtil.parseFrame;

public class GroupedPctTest {

  @Test public void testGID() {
    //Key key = Key.make("cars.hex");
    //Frame fr = parseFrame(key, "./smalldata/cars.csv");
//    GroupedPct pct = new GroupedPct(fr, ,);

  }

  public static void main(String[] args) throws Exception {
    water.Boot.main(GroupedPctTest.class, args);
  }

  public static void userMain(String[] args) throws Exception {
    H2O.main(args);
    new GroupedPctTest().testGID();
    System.exit(0);
  }
}
