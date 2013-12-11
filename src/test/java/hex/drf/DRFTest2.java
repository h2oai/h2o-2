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
        a( a(154871,  52304,    18,    0,   24,   15,   935),  
           a( 34958, 240871,  1423,   21,  243,  656,    56),  
           a(     4,   3836, 30040,  203,    1, 1049,     0),  
           a(     0,      8,   640, 1960,    0,   81,     0),  
           a(   229,   6824,   126,    0, 2137,   17,     0),  
           a(    21,   4278,  5353,   91,    2, 7332,     0),  
           a(  7170,    297,     0,    0,    0,    0, 12718)),
        s("1", "2", "3", "4", "5", "6", "7"),
        50/*max_depth*/);
  }
}
