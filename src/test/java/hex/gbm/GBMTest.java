package hex.gbm;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import hex.rf.*;
import hex.rf.ConfusionTask.CMFinal;
import hex.rf.ConfusionTask.CMJob;
import hex.rf.DRF.DRFJob;
import hex.rf.Tree.StatType;
import java.io.File;
import java.util.Arrays;
import org.junit.BeforeClass;
import org.junit.Test;
import water.*;
import water.api.RequestBuilders.Response;
import water.fvec.*;

public class GBMTest extends TestUtil {

  @BeforeClass public static void stall() { stall_till_cloudsize(1); }

  private abstract class PrepData { abstract Vec prep(Frame fr); }

  /*@Test*/ public void testBasicGBM() {
    // Disabled Regression tests
    //basicDRF("./smalldata/cars.csv","cars.hex",
    //         new PrepData() { Vec prep(Frame fr) { UKV.remove(fr.remove("name")._key); return fr.remove("economy (mpg)"); } 
    //         });
    //basicGBM("./smalldata/cars.csv","cars.hex",
    //         new PrepData() { Vec prep(Frame fr) { UKV.remove(fr.remove("name")._key); return fr.remove("economy (mpg)"); } 
    //         });

    // Classification tests
    basicGBM("./smalldata/test/test_tree.csv","tree.hex",
             new PrepData() { Vec prep(Frame fr) { return fr.remove(1); } 
             });
    basicGBM("./smalldata/logreg/prostate.csv","prostate.hex",
             new PrepData() {
               Vec prep(Frame fr) { 
                 assertEquals(380,fr.numRows());
                 // Remove patient ID vector
                 UKV.remove(fr.remove("ID")._key); 
                 // Prostate: predict on CAPSULE
                 return fr.remove("CAPSULE");
               }
             });
    basicGBM("./smalldata/cars.csv","cars.hex",
             new PrepData() { Vec prep(Frame fr) { UKV.remove(fr.remove("name")._key); return fr.remove("cylinders"); } 
             });
    basicGBM("./smalldata/airlines/allyears2k_headers.zip","air.hex",
             new PrepData() { Vec prep(Frame fr) { return fr.remove("IsDepDelayed"); }
             });
    basicGBM("../datasets/UCI/UCI-large/covtype/covtype.data","covtype.hex",
             new PrepData() {
               Vec prep(Frame fr) { 
                 assertEquals(581012,fr.numRows());
                 // Covtype: predict on last column
                 return fr.remove(54);
               }
             });
  }

  // ==========================================================================
  public void basicGBM(String fname, String hexname, PrepData prep) {
    File file = TestUtil.find_test_file(fname);
    if( file == null ) return;  // Silently abort test if the file is missing
    Key fkey = NFSFileVec.make(file);
    Key dest = Key.make(hexname);
    GBM gbm = null;
    try {
      gbm = new GBM();
      gbm.source = ParseDataset2.parse(dest,new Key[]{fkey});
      UKV.remove(fkey);
      gbm.vresponse = prep.prep(gbm.source);
      gbm.ntrees = 5;
      gbm.max_depth = 8;
      gbm.learn_rate = 0.1f;
      gbm.min_rows=1;
      gbm.nbins = 4;
      gbm.serve();              // Start it
      gbm.get();                // Block for it

    } finally {
      UKV.remove(dest);         // Remove original hex frame key
      if( gbm != null ) {
        UKV.remove(gbm.dest()); // Remove the model
        UKV.remove(gbm.vresponse._key);
        gbm.remove();           // Remove GBM Job
      }
    }
  }

  static final int IGNS[] = new int[] {
                       6, 7, 8, 9,
    10,11,12,13,14,15,16,17,18,19,
    20,21,22,23,24,25,26,27,28,29,
    30,31,32,33,34,35,36,37,38,39,
    40,41,42,43,44,45,46,47,48,49,
  };
  @Test public void testBasicDRF() {
    // Disabled Regression tests
    //basicDRF("./smalldata/cars.csv","cars.hex",
    //         new PrepData() { Vec prep(Frame fr) { UKV.remove(fr.remove("name")._key); return fr.remove("economy (mpg)"); } 
    //         });

    // Classification tests
    basicDRF("./smalldata/test/test_manycol_tree.csv","tree.hex",
             new PrepData() { Vec prep(Frame fr) { return fr.remove(fr.numCols()-1); } 
             });
    //basicDRF("./smalldata/logreg/prostate.csv","prostate.hex",
    //         new PrepData() {
    //           Vec prep(Frame fr) { 
    //             assertEquals(380,fr.numRows());
    //             // Remove patient ID vector
    //             UKV.remove(fr.remove("ID")._key); 
    //             // Prostate: predict on CAPSULE
    //             return fr.remove("CAPSULE");
    //           }
    //         });
    //basicDRF("./smalldata/iris/iris_wheader.csv","iris.hex",
    //         new PrepData() { Vec prep(Frame fr) { return fr.remove("class"); } 
    //         });
    //basicDRF("./smalldata/airlines/allyears2k_headers.zip","airlines.hex",
    //         new PrepData() { Vec prep(Frame fr) { 
    //           UKV.remove(fr.remove("IsArrDelayed")._key); 
    //           return fr.remove("IsDepDelayed"); 
    //         }
    //         });
    //basicDRF("./smalldata/cars.csv","cars.hex",
    //         new PrepData() { Vec prep(Frame fr) { UKV.remove(fr.remove("name")._key); return fr.remove("cylinders"); } 
    //         });
    //basicDRF("./smalldata/airlines/allyears2k_headers.zip","air.hex",
    //         new PrepData() { Vec prep(Frame fr) { return fr.remove("IsDepDelayed"); }
    //         });
    //basicDRF("../datasets/UCI/UCI-large/covtype/covtype.data","covtype.hex",
    //         //basicDRF("./smalldata/covtype/covtype.20k.data","covtype.hex",
    //         new PrepData() {
    //           Vec prep(Frame fr) {
    //             for( int ign : IGNS )
    //               UKV.remove(fr.remove(Integer.toString(ign))._key);
    //             // Covtype: predict on last column
    //             return fr.remove(fr.numCols()-1);
    //           }
    //         });
  }

  public void basicDRF(String fname, String hexname, PrepData prep) {
    File file = TestUtil.find_test_file(fname);
    if( file == null ) return;  // Silently abort test if the file is missing
    Key fkey = NFSFileVec.make(file);
    Key dest = Key.make(hexname);
    DRF drf = null;
    try {
      drf = new DRF();
      drf.source = ParseDataset2.parse(dest,new Key[]{fkey});
      UKV.remove(fkey);
      drf.vresponse = prep.prep(drf.source);
      drf.ntrees = 4;
      drf.max_depth = 50;
      drf.min_rows=1;
      drf.nbins = 1024;
      drf.mtries = -1;
      drf.sample_rate = 0.66667f;   // No sampling
      drf.seed = (1L<<32)|2;
      drf.serve();              // Start it
      drf.get();                // Block for it
      System.out.println(new String(drf.writeJSON(new AutoBuffer()).buf()));

    } finally {
      UKV.remove(dest);         // Remove whole frame
      if( drf != null ) {
        UKV.remove(drf.dest()); // Remove the model
        UKV.remove(drf.vresponse._key);
        drf.remove();
      }
    }
  }

  /*@Test*/ public void testCovtype() {
    //Key okey = loadAndParseFile("covtype.hex", "smalldata/covtype/covtype.20k.data");
    Key okey = loadAndParseFile("covtype.hex", "../datasets/UCI/UCI-large/covtype/covtype.data");
    //Key okey = loadAndParseFile("covtype.hex", "/home/0xdiag/datasets/standard/covtype.data");
    ValueArray val = UKV.get(okey);

    // setup default values for DRF
    int ntrees  = 4;
    int depth   = 50;
    int gini    = StatType.ENTROPY.ordinal();
    int seed    = 42;
    StatType statType = StatType.values()[gini];
    // Setup all columns, minus an ignored set
    final int cols[] = new int[val.numCols()-IGNS.length];
    int x=0;
    for( int i=0; i<val.numCols(); i++ ) {
      if( x < IGNS.length && IGNS[x] == i ) x++;
      else cols[i-x]=i;
    }
    System.out.println("RF1 cols="+Arrays.toString(cols));

    // Start the distributed Random Forest
    final Key modelKey = Key.make("model");
    DRFJob result = hex.rf.DRF.execute(modelKey,cols,val,ntrees,depth,1024,statType,seed, true, null, -1, Sampling.Strategy.RANDOM, 0.66667f, null, 1/*verbose*/, 0, false);
    // Wait for completion on all nodes
    RFModel model = result.get();
    CMJob cmjob = ConfusionTask.make( model, val._key, cols[cols.length-1], null, false);
    CMFinal cm = cmjob.get(); // block until CM is computed
    cm.report();
    UKV.remove(cmjob.dest());

    assertEquals("Number of classes", 7,  model.classes());
    assertEquals("Number of trees", ntrees, model.size());

    model.deleteKeys();
    UKV.remove(modelKey);
    UKV.remove(okey);
  }
}
