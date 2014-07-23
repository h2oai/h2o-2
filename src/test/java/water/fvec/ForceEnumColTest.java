package water.fvec;

import org.junit.Test;
import water.H2O;
import water.TestUtil;

public class ForceEnumColTest extends TestUtil {
  private double[] d(double... ds) { return ds; }

  // Parse a dataset where the last column is forced to be interpreted as an Enum col, despite
  // being full of zeros (which are intended to be NAs for this dataset)
  @Test public void testForceEnum() {
    int old = H2O.OPT_ARGS.forceEnumCol;
    H2O.OPT_ARGS.forceEnumCol = 3;
    Frame fr = parseFrame(null,"smalldata/test/is_NA2.csv");
    double[][] exp = new double[][] {
      d( 1,0,0),
      d( 2,1,0),
      d( 3,2,1),
      d( 4,0,0),
      d( 5,1,0),
      d( 6,2,1),
      d( 7,0,0),
      d( 8,1,0),
      d( 9,2,0),
      d(10,0,2),
      d(11,1,0),
      d(12,2,0),
    };

    try {
      System.out.println("--- Forced Frame \n" + fr.toStringAll());
      ParserTest2.testParsed(fr, exp, exp.length);
    } finally {
      H2O.OPT_ARGS.forceEnumCol = old;
      fr.delete();
    }
  }
}
