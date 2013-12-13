package hex.drf;

import org.junit.BeforeClass;
import org.junit.Test;
import water.TestUtil;
import water.fvec.Frame;
import water.UKV;

public class DRFTest2 extends TestUtil {

  @BeforeClass public static void stall() { stall_till_cloudsize(1); }

  static final String[] s(String...arr)  { return arr; }
  static final long[]   a(long ...arr)   { return arr; }
  static final long[][] a(long[] ...arr) { return arr; }

  // A bigger DRF test, useful for tracking memory issues.
  @Test public void testAirlines() throws Throwable {
    new DRFTest().basicDRF(
        //
        //"../demo/c5/row10000.csv.gz", "c5.hex", null, null, 
        "../datasets/UCI/UCI-large/covtype/covtype.data", "covtype.hex", null, null,
        //"./smalldata/iris/iris_wheader.csv", "iris.hex", null, null,
        new DRFTest.PrepData() { @Override int prep(Frame fr) { return fr.numCols()-1; } },
        10/*ntree*/,
        a( a(199145,   7055,     8,    0,  115,    22,   489), 
           a(  9892, 265406,   434,    3,  456,   251,   161), 
           a(    11,    748, 33254,  135,   23,   636,     1), 
           a(     0,      3,   372, 2214,    0,    59,     0), 
           a(   169,   1739,   118,    1, 7115,    38,     4), 
           a(    30,    654,  1458,  107,   37, 14512,     0), 
           a(  1057,    156,     0,    0,    8,     0, 18838)),
        s("1", "2", "3", "4", "5", "6", "7"),
        50/*max_depth*/);
  }
}
