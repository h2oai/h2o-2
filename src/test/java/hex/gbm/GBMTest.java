package hex.gbm;

import static org.junit.Assert.assertEquals;
import hex.rf.*;
import hex.rf.ConfusionTask.CMFinal;
import hex.rf.ConfusionTask.CMJob;
import hex.rf.DRF.DRFJob;
import hex.rf.Tree.StatType;

import java.io.File;
import java.util.Arrays;

import org.junit.*;

import water.*;
import water.api.ConfusionMatrix;
import water.fvec.*;

@Ignore
public class GBMTest extends TestUtil {

  @BeforeClass public static void stall() { stall_till_cloudsize(1); }

  private abstract class PrepData { abstract int prep(Frame fr); }

  @Test public void testBasicGBM() {
    // Disabled Regression tests
    //basicDRF("./smalldata/cars.csv","cars.hex",
    //         new PrepData() { int prep(Frame fr) { UKV.remove(fr.remove("name")._key); return fr.remove("economy (mpg)"); }
    //         });
    //basicGBM("./smalldata/cars.csv","cars.hex",
    //         new PrepData() { int prep(Frame fr) { UKV.remove(fr.remove("name")._key); return fr.remove("economy (mpg)"); }});

    // Classification tests
    basicGBM("./smalldata/test/test_tree.csv","tree.hex",
             new PrepData() { int prep(Frame fr) { return 1; }
             });

    basicGBM("./smalldata/test/test_tree_minmax.csv","tree_minmax.hex",
             new PrepData() { int prep(Frame fr) { return fr.find("response"); }
             });
    basicGBM("./smalldata/logreg/prostate.csv","prostate.hex",
             new PrepData() {
               int prep(Frame fr) {
                 assertEquals(380,fr.numRows());
                 // Remove patient ID vector
                 UKV.remove(fr.remove("ID")._key);
                 // Prostate: predict on CAPSULE
                 return fr.find("CAPSULE");
               }
             });
    basicGBM("./smalldata/cars.csv","cars.hex",
             new PrepData() { int prep(Frame fr) { UKV.remove(fr.remove("name")._key); return fr.find("cylinders"); }
             });
    basicGBM("./smalldata/airlines/allyears2k_headers.zip","air.hex",
             new PrepData() { int prep(Frame fr) { return fr.find("IsDepDelayed"); }
             });
    //basicGBM("../datasets/UCI/UCI-large/covtype/covtype.data","covtype.hex",
    //         new PrepData() {
    //           int prep(Frame fr) {
    //             assertEquals(581012,fr.numRows());
    //             for( int ign : IGNS )
    //               UKV.remove(fr.remove("C"+Integer.toString(ign))._key);
    //             // Covtype: predict on last column
    //             return fr.numCols()-1;
    //           }
    //         });
  }

  // ==========================================================================
  public void basicGBM(String fname, String hexname, PrepData prep) {
    File file = TestUtil.find_test_file(fname);
    if( file == null ) return;  // Silently abort test if the file is missing
    Key fkey = NFSFileVec.make(file);
    Key dest = Key.make(hexname);
    GBM gbm = null;
    Frame fr = null;
    try {
      gbm = new GBM();
      gbm.source = fr = ParseDataset2.parse(dest,new Key[]{fkey});
      UKV.remove(fkey);
      gbm.classification = true;
      int idx = prep.prep(fr);
      String rname =fr._names[idx];
      gbm.response = fr.vecs()[idx];
      System.out.println("i="+idx+" "+gbm.response);
      fr.remove(idx);           // Move response to the end
      fr.add(rname,gbm.response);
      gbm.ntrees = 5;
      gbm.max_depth = 5;
      gbm.min_rows = 1;
      gbm.nbins = 100;
      gbm.cols = new int[fr.numCols()];
      for( int i=0; i<gbm.cols.length; i++ ) gbm.cols[i]=i;
      gbm.learn_rate = .2f;
      gbm.invoke();

      fr = gbm.score(gbm.source);

    } finally {
      UKV.remove(dest);         // Remove original hex frame key
      if( gbm != null ) {
        UKV.remove(gbm.dest()); // Remove the model
        UKV.remove(gbm.response._key);
        gbm.remove();           // Remove GBM Job
        if( fr != null ) fr.remove();
      }
    }
  }

  static final int IGNS[] = new int[] { // covtype ignorable columns just while debugging DRF issues
                       6, 7, 8, 9,
    10,11,12,13,14,15,16,17,18,19,
    20,21,22,23,24,25,26,27,28,29,
    30,31,32,33,34,35,36,37,38,39,
    40,41,42,43,44,45,46,47,48,49,
  };
  /*@Test*/ public void testBasicDRF() {
    // Disabled Regression tests
    //basicDRF("./smalldata/cars.csv","cars.hex",
    //         new PrepData() { int prep(Frame fr) { UKV.remove(fr.remove("name")._key); return fr.remove("economy (mpg)"); }
    //         });

    // Classification tests
    basicDRF("./smalldata/test/test_tree.csv","tree.hex",
             new PrepData() { int prep(Frame fr) { return fr.numCols()-1; }
             });

    //basicDRF("./smalldata/logreg/prostate.csv","prostate.hex",
    //         new PrepData() {
    //           int prep(Frame fr) {
    //             assertEquals(380,fr.numRows());
    //             // Remove patient ID vector
    //             UKV.remove(fr.remove("ID")._key);
    //             // Prostate: predict on CAPSULE
    //             return fr.remove("CAPSULE");
    //           }
    //         });
    //basicDRF("./smalldata/iris/iris_wheader.csv","iris.hex",
    //         new PrepData() { int prep(Frame fr) { return fr.remove("class"); }
    //         });
    //basicDRF("./smalldata/airlines/allyears2k_headers.zip","airlines.hex",
    //         new PrepData() { int prep(Frame fr) {
    //           UKV.remove(fr.remove("IsArrDelayed")._key);
    //           return fr.remove("IsDepDelayed");
    //         }
    //         });
    //basicDRF("./smalldata/cars.csv","cars.hex",
    //         new PrepData() { int prep(Frame fr) { UKV.remove(fr.remove("name")._key); return fr.remove("cylinders"); }
    //         });
    //basicDRF("./smalldata/airlines/allyears2k_headers.zip","air.hex",
    //         new PrepData() { int prep(Frame fr) { return fr.remove("IsDepDelayed"); }
    //         });
    //basicDRF("../datasets/UCI/UCI-large/covtype/covtype.data","covtype.hex",
    //         //basicDRF("./smalldata/covtype/covtype.20k.data","covtype.hex",
    //         new PrepData() {
    //           int prep(Frame fr) {
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
    Frame fr = null;
    try {
      drf = new DRF();
      drf.classification = true;
      fr = drf.source = ParseDataset2.parse(dest,new Key[]{fkey});
      UKV.remove(fkey);
      int idx = prep.prep(fr);
      String rname =fr._names[idx];
      drf.response = fr.vecs()[idx];
      fr.remove(idx);           // Move response to the end
      fr.add(rname,drf.response);

      drf.ntrees = 2;
      drf.max_depth = 50;
      drf.min_rows = 1;
      drf.nbins = 100;
      drf.mtries = -1;
      drf.sample_rate = 0.66667f;   // No sampling
      drf.seed = (1L<<32)|2;
      drf.invoke();

      fr = drf.score(drf.source);

    } finally {
      UKV.remove(dest);         // Remove whole frame
      if( drf != null ) {
        UKV.remove(drf.dest()); // Remove the model
        UKV.remove(drf.response._key);
        drf.remove();
        if( fr != null ) fr.remove();
      }
    }
  }

  /*@Test*/ public void testCovtype() {
    //Key okey = loadAndParseFile("covtype.hex", "smalldata/covtype/covtype.20k.data");
    Key okey = loadAndParseFile("covtype.hex", "../datasets/UCI/UCI-large/covtype/covtype.data");
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

  // Test-on-Train.  Slow test, needed to build a good model.
  @Test public void testGBMTrainTest() {
    File file1 = TestUtil.find_test_file("..//classifcation1Train.txt");
    if( file1 == null ) return; // Silently ignore if file not found
    Key fkey1 = NFSFileVec.make(file1);
    Key dest1 = Key.make("train.hex");
    File file2 = TestUtil.find_test_file("..//classification1Test.txt");
    Key fkey2 = NFSFileVec.make(file2);
    Key dest2 = Key.make("test.hex");
    GBM gbm = null;
    Frame fr = null, fpreds = null;
    try {
      gbm = new GBM();
      fr = ParseDataset2.parse(dest1,new Key[]{fkey1});
      UKV.remove(fkey1);
      UKV.remove(fr.remove("agentId")._key); // Remove unique ID; too predictive
      gbm.response = fr.remove("outcome");  // Train on the outcome
      gbm.source = fr;
      gbm.ntrees = 5;
      gbm.max_depth = 10;
      gbm.learn_rate = 0.2f;
      gbm.min_rows = 10;
      gbm.nbins = 100;
      gbm.invoke();

      // Test on the train data
      Frame ftest = ParseDataset2.parse(dest2,new Key[]{fkey2});
      UKV.remove(fkey2);
      fpreds = gbm.score(ftest);

      // Build a confusion matrix
      ConfusionMatrix CM = new ConfusionMatrix();
      CM.actual = ftest;
      CM.vactual = ftest.vecs()[ftest.find("outcome")];
      CM.predict = fpreds;
      CM.vpredict = fpreds.vecs()[fpreds.find("predict")];
      CM.serve();               // Start it, do it

      // Really crappy cut-n-paste of what should be in the ConfusionMatrix class itself
      long cm[][] = CM.cm;
      long acts [] = new long[cm   .length];
      long preds[] = new long[cm[0].length];
      for( int a=0; a<cm.length; a++ ) {
        long sum=0;
        for( int p=0; p<cm[a].length; p++ ) { sum += cm[a][p]; preds[p] += cm[a][p]; }
        acts[a] = sum;
      }
      String adomain[] = ConfusionMatrix.show(acts ,CM.vactual .domain());
      String pdomain[] = ConfusionMatrix.show(preds,CM.vpredict.domain());

      StringBuilder sb = new StringBuilder();
      sb.append("Act/Prd\t");
      for( String s : pdomain )
        if( s != null )
          sb.append(s).append('\t');
      sb.append("Error\n");

      long terr=0;
      for( int a=0; a<cm.length; a++ ) {
        if( adomain[a] == null ) continue;
        sb.append(adomain[a]).append('\t');
        long correct=0;
        for( int p=0; p<pdomain.length; p++ ) {
          if( pdomain[p] == null ) continue;
          if( adomain[a].equals(pdomain[p]) ) correct = cm[a][p];
          sb.append(cm[a][p]).append('\t');
        }
        long err = acts[a]-correct;
        terr += err;            // Bump totals
        sb.append(String.format("%5.3f = %d / %d\n", (double)err/acts[a], err, acts[a]));
      }
      sb.append("Totals\t");
      for( int p=0; p<pdomain.length; p++ )
        if( pdomain[p] != null )
          sb.append(preds[p]).append("\t");
      sb.append(String.format("%5.3f = %d / %d\n", (double)terr/CM.vactual.length(), terr, CM.vactual.length()));

      System.out.println(sb);

    } finally {
      UKV.remove(dest1);        // Remove original hex frame key
      UKV.remove(fkey2);
      UKV.remove(dest2);
      if( gbm != null ) {
        UKV.remove(gbm.dest()); // Remove the model
        UKV.remove(gbm.response._key);
        gbm.remove();           // Remove GBM Job
      }
      if( fr != null ) fr.remove();
      if( fpreds != null ) fpreds.remove();
    }
  }

  // Adapt a trained model to a test dataset with different enums
  /*@Test*/ public void testModelAdapt() {
    File file1 = TestUtil.find_test_file("./smalldata/kaggle/KDDTrain.arff.gz");
    Key fkey1 = NFSFileVec.make(file1);
    Key dest1 = Key.make("KDDTrain.hex");
    File file2 = TestUtil.find_test_file("./smalldata/kaggle/KDDTest.arff.gz");
    Key fkey2 = NFSFileVec.make(file2);
    Key dest2 = Key.make("KDDTest.hex");
    GBM gbm = null;
    Frame fr = null;
    try {
      gbm = new GBM();
      gbm.source = ParseDataset2.parse(dest1,new Key[]{fkey1});
      UKV.remove(fkey1);
      gbm.response = gbm.source.remove(41); // Response is col 41
      gbm.ntrees = 2;
      gbm.max_depth = 8;
      gbm.learn_rate = 0.2f;
      gbm.min_rows = 10;
      gbm.nbins = 100;
      gbm.invoke();

      // The test data set has a few more enums than the train
      Frame ftest = ParseDataset2.parse(dest2,new Key[]{fkey2});
      Frame preds = gbm.score(ftest);

    } finally {
      UKV.remove(dest1);        // Remove original hex frame key
      if( gbm != null ) {
        UKV.remove(gbm.dest()); // Remove the model
        UKV.remove(gbm.response._key);
        gbm.remove();           // Remove GBM Job
        if( fr != null ) fr.remove();
      }
    }
  }
}
