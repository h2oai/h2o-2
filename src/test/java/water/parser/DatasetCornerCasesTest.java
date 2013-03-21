package water.parser;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import hex.rf.*;
import hex.rf.DRF.DRFFuture;
import hex.rf.Tree.StatType;
import org.junit.Test;
import water.*;
import water.DRemoteTask.DFuture;
import water.parser.ParseDataset;

public class DatasetCornerCasesTest extends TestUtil {

  /*
   * HTWO-87 bug test
   *
   *  - two lines dataset (one line is a comment) throws assertion java.lang.AssertionError: classOf no dists > 0? 1
   */
  @Test public void testTwoLineDataset() throws Exception {
    Key fkey = load_test_file("smalldata/test/HTWO-87-two-lines-dataset.csv");
    Key okey = Key.make("HTWO-87-two-lines-dataset.hex");
    ParseDataset.parse(okey,DKV.get(fkey));
    UKV.remove(fkey);
    ValueArray val = DKV.get(okey).get();

    // Check parsed dataset
    assertEquals("Number of chunks == 1", 1, val.chunks());
    assertEquals("Number of rows   == 2", 2, val._numrows);
    assertEquals("Number of cols   == 9", 9, val._cols.length);

    // setup default values for DRF
    int ntrees  = 5;
    int depth   = 30;
    int gini    = StatType.GINI.ordinal();
    long seed   =  42L;
    StatType statType = StatType.values()[gini];
    final int num_cols = val.numCols();
    final int classcol = num_cols-1; // Classify the last column
    int cols[] = new int[]{0,1,2,3,4,5,6,7,8};

    // Start the distributed Random Forest
    try {
      final Key modelKey = Key.make("model");
      DRFFuture result = hex.rf.DRF.execute(modelKey, cols, val,
                                   ntrees,depth,1.0f,(short)1024,statType,seed,true,null,-1,false,null,0,0);
      // Just wait little bit
      result.get();
      // Create incremental confusion matrix
      RFModel model = UKV.get(modelKey);
      assertEquals("Number of classes == 1", 1,  model.classes());
      assertTrue("Number of trees > 0 ", model.size()> 0);
      model.deleteKeys();
    } catch( IllegalArgumentException e ) {
      assertEquals("java.lang.IllegalArgumentException: Found 1 classes: Response column must be an integer in the interval [2,254]",e.toString());
    }
    UKV.remove(okey);
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
      String keyname     = test_prefix + tests[i] + ".hex";
      testOneLineDataset(datasetFilename, keyname);
    }
  }

  private void testOneLineDataset(String filename, String keyname) {
    Key fkey = load_test_file(filename);
    Key okey = Key.make(keyname);
    ParseDataset.parse(okey,DKV.get(fkey));

    ValueArray val = DKV.get(okey).get();
    assertEquals(filename + ": number of chunks == 1", 1, val.chunks());
    assertEquals(filename + ": number of rows   == 2", 2, val._numrows);
    assertEquals(filename + ": number of cols   == 9", 9, val._cols.length);

    UKV.remove(fkey);
    UKV.remove(okey);
  }
}
