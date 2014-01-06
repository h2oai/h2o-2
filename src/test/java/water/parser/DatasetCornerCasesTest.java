package water.parser;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

import hex.drf.DRFTest;
import hex.drf.DRFTest.PrepData;
import org.junit.Test;
import water.api.Constants.Extensions;
import water.*;
import water.fvec.Frame;

public class DatasetCornerCasesTest extends TestUtil {

  static final String[] s(String...arr)  { return arr; }
  static final long[]   a(long ...arr)   { return arr; }
  static final long[][] a(long[] ...arr) { return arr; }

  /*
   * HTWO-87 bug test
   *
   *  - two lines dataset (one line is a comment) throws assertion java.lang.AssertionError: classOf no dists > 0? 1
   */
  @Test public void testTwoLineDataset() throws Throwable {
    String hexnametrain = "HTWO-87-two-lines-dataset.hex";
    String fnametrain = "smalldata/test/HTWO-87-two-lines-dataset.csv";
    Key okey = Key.make(hexnametrain);
    Frame val = parseFrame(okey,fnametrain);

    // Check parsed dataset
    assertEquals("Number of chunks == 1", 1, val.anyVec().nChunks());
    assertEquals("Number of rows   == 2", 2, val.numRows());
    assertEquals("Number of cols   == 9", 9, val.numCols());
    UKV.remove(okey);

    // DRF2 - a little more complete inspection of data
    try { 
      new DRFTest().basicDRF(
        fnametrain, hexnametrain, null, null,
        new hex.drf.DRFTest.PrepData() { @Override public int prep(Frame fr) { return fr.numCols()-1; } },
        5/*ntree*/,
        a( a( 0, 0 ),
           a( 0, 0 )),
        s("1", "2"),

        30/*max_depth*/,
        1024/*nbins*/,
        1.0f/*sample_rate*/,
        false/*print_throws*/,
        0 /*optflag*/  );
      assertTrue(false);
    } catch( IllegalArgumentException iae ) {
      assertEquals("java.lang.IllegalArgumentException: Constant response column!",iae.toString());
    }
  }

  /* The following tests deal with one line dataset ended by different number of newlines. */

  /*
   * HTWO-87-related bug test
   *
   *  - only one line dataset - guessing parser should recognize it.
   *  - this datasets are ended by different number of \n (0x0A):
   *    - HTWO-87-one-line-dataset-0.csv    - the line is NOT ended by \n
   *    - HTWO-87-one-line-dataset-1.csv    - the line is ended by 1 \n     (0x0A)
   *    - HTWO-87-one-line-dataset-2.csv    - the line is ended by 2 \n     (0x0A 0x0A)
   *    - HTWO-87-one-line-dataset-1dos.csv - the line is ended by \r\n     (0x0D 0x0A)
   *    - HTWO-87-one-line-dataset-2dos.csv - the line is ended by 2 \r\n   (0x0D 0x0A 0x0D 0x0A)
   */
  @Test public void testOneLineDataset() {
    // max number of dataset files
    final String tests[] = {"0", "1unix", "2unix", "1dos", "2dos" };
    final String test_dir    = "smalldata/test/";
    final String test_prefix = "HTWO-87-one-line-dataset-";

    for (int i = 0; i < tests.length; i++) {
      String datasetFilename = test_dir + test_prefix + tests[i] + ".csv";
      String keyname     = test_prefix + tests[i] + Extensions.HEX;
      testOneLineDataset(datasetFilename, keyname);
    }
  }

  private void testOneLineDataset(String filename, String keyname) {
    Key okey = Key.make(keyname);
    Frame val = parseFrame(okey,filename);
    assertEquals(filename + ": number of chunks == 1", 1, val.anyVec().nChunks());
    assertEquals(filename + ": number of rows   == 2", 2, val.numRows());
    assertEquals(filename + ": number of cols   == 9", 9, val.numCols());

    UKV.remove(okey);
  }
}
