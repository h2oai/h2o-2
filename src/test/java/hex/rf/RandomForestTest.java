package hex.rf;

import static org.junit.Assert.assertEquals;
import hex.rf.DRF.DRFJob;
import hex.rf.Tree.StatType;

import org.junit.BeforeClass;

import water.*;

public class RandomForestTest extends TestUtil {

  @BeforeClass public static void stall() { stall_till_cloudsize(1); }

  // Test kaggle/creditsample-test data
  @org.junit.Test public void kaggle_credit() {
    Key okey = loadAndParseFile("credit.hex", "smalldata/kaggle/creditsample-training.csv.gz");
    ValueArray val = DKV.get(okey).get();

    // Check parsed dataset
    final int n = new int[]{4,2,1}[ValueArray.LOG_CHK-20];
    assertEquals("Number of chunks", n, val.chunks());
    assertEquals("Number of rows", 150000, val.numRows());
    assertEquals("Number of cols", 12, val.numCols());

    // setup default values for DRF
    int ntrees  = 3;
    int depth   = 30;
    int gini    = StatType.GINI.ordinal();
    int seed    = 42;
    StatType statType = StatType.values()[gini];
    final int cols[] = new int[]{0,2,3,4,5,7,8,9,10,11,1}; // ignore column 6, classify column 1

    // Start the distributed Random Forest
    final Key modelKey = Key.make("model");
    DRFJob result = hex.rf.DRF.execute(modelKey,cols,val,ntrees,depth,1024,statType,seed, true, null, -1, Sampling.Strategy.RANDOM, 1.0f, null, 0, 0, false);
    // Wait for completion on all nodes
    RFModel model = result.get();

    assertEquals("Number of classes", 2,  model.classes());
    assertEquals("Number of trees", ntrees, model.size());

    model.delete();
    val.delete();
  }

  /*@org.junit.Test*/ public void covtype() {
    Key okey = loadAndParseFile("covtype.hex", "smalldata/covtype/covtype.20k.data");
    //Key okey = loadAndParseFile("covtype.hex", "../datasets/UCI/UCI-large/covtype/covtype.data");
    //Key okey = loadAndParseFile("covtype.hex", "/home/0xdiag/datasets/standard/covtype.data");
    //Key okey = loadAndParseFile("mnist.hex", "/home/0xdiag/datasets/mnist/mnist8m.csv");
    ValueArray val = UKV.get(okey);

    // setup default values for DRF
    int ntrees  = 8;
    int depth   = 999;
    int gini    = StatType.ENTROPY.ordinal();
    int seed    = 42;
    StatType statType = StatType.values()[gini];
    final int cols[] = new int[val.numCols()];
    for( int i=1; i<cols.length; i++ ) cols[i]=i-1;
    cols[cols.length-1]=0;      // Class is in column 0 for mnist

    // Start the distributed Random Forest
    final Key modelKey = Key.make("model");
    DRFJob result = hex.rf.DRF.execute(modelKey,cols,val,ntrees,depth,1024,statType,seed, true, null, -1, Sampling.Strategy.RANDOM, 1.0f, null, 0, 0, false);
    // Wait for completion on all nodes
    RFModel model = result.get();

    assertEquals("Number of classes", 10,  model.classes());
    assertEquals("Number of trees", ntrees, model.size());

    model.delete();
    val.delete();
  }

}
