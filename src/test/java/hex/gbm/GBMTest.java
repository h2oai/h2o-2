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

public class GBMTest extends TestUtil {

  @BeforeClass public static void stall() { stall_till_cloudsize(1); }

  private abstract class PrepData { abstract int prep(Frame fr); }

  @Test
  public void testBasicGBM() {
    // Regression tests
    basicGBM("./smalldata/cars.csv","cars.hex",
             new PrepData() { int prep(Frame fr ) { UKV.remove(fr.remove("name")._key); return ~fr.find("economy (mpg)"); }});

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
      int idx = prep.prep(fr);
      if( idx < 0 ) { gbm.classification = false; idx = ~idx; }
      String rname =fr._names[idx];
      gbm.response = fr.vecs()[idx];
      fr.remove(idx);           // Move response to the end
      fr.add(rname,gbm.response);
      gbm.ntrees = 4;
      gbm.max_depth = 4;
      gbm.min_rows = 1;
      gbm.nbins = 50;
      gbm.cols = new int[fr.numCols()];
      for( int i=0; i<gbm.cols.length; i++ ) gbm.cols[i]=i;
      gbm.learn_rate = .2f;
      gbm.invoke();

      fr = gbm.score(gbm.source);

      GBM.GBMModel gbmmodel = UKV.get(gbm.dest());
      //System.out.println(gbmmodel.toJava());

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
      gbm.nbins = 50;
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
