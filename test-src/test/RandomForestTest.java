package test;
import static org.junit.Assert.assertEquals;
import hex.rf.*;
import hex.rf.Tree.StatType;

import org.junit.BeforeClass;

import water.*;
import water.parser.ParseDataset;

public class RandomForestTest extends TestUtil {

  @BeforeClass public static void stall() { stall_till_cloudsize(3); }

  // Test kaggle/creditsample-test data
  @org.junit.Test public void kaggle_credit() throws Exception {
    Key fkey = load_test_file("smalldata/kaggle/creditsample-training.csv.gz");
    Key okey = Key.make("credit.hex");
    ParseDataset.parse(okey,DKV.get(fkey));
    UKV.remove(fkey);
    UKV.remove(Key.make("smalldata/kaggle/creditsample-training.csv.gz_UNZIPPED"));
    UKV.remove(Key.make("smalldata\\kaggle\\creditsample-training.csv.gz_UNZIPPED"));
    ValueArray val = ValueArray.value(okey);

    // Check parsed dataset
    assertEquals("Number of chunks", 4, val.chunks());
    assertEquals("Number of rows", 150000, val.numRows());
    assertEquals("Number of cols", 12, val.numCols());

    // setup default values for DRF
    int ntrees  = 3;
    int depth   = 30;
    int gini    = StatType.GINI.ordinal();
    int seed =  42;
    StatType statType = StatType.values()[gini];
    final int cols[] = new int[]{0,2,3,4,5,7,8,9,10,11,1}; // ignore column 6, classify column 1

    // Start the distributed Random Forest
    final Key modelKey = Key.make("model");
    DRF drf = hex.rf.DRF.webMain(modelKey,cols,val,ntrees,depth,1.0f,(short)1024,statType,seed, true, null, -1, false, null, 0, 0);
    // Just wait little bit
    drf.get();
    // Create incremental confusion matrix.
    RFModel model;
    while( true ) {
      // RACEY BUG HERE: Model is supposed to be complete after drf.get, but as
      // of 11/5/2012 it takes a little while for all trees to appear.
      model = UKV.get(modelKey, new RFModel());
      if( model.size()==ntrees ) break;
      Thread.sleep(100);
    }
    assertEquals("Number of classes", 2,  model.classes());
    assertEquals("Number of trees", ntrees, model.size());

    model.deleteKeys();
    UKV.remove(modelKey);
    UKV.remove(okey);
  }
}
