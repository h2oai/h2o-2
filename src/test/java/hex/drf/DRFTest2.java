package hex.drf;

import junit.framework.Assert;
import hex.drf.DRF.DRFModel;
import hex.trees.TreeTestWithBalanceAndCrossVal;

import org.junit.*;

import water.*;
import water.fvec.Frame;
import water.fvec.Vec;

public class DRFTest2 extends TreeTestWithBalanceAndCrossVal {

  //@BeforeClass public static void stall() { stall_till_cloudsize(1); }

  // A bigger DRF test, useful for tracking memory issues.
  /*@Test*/ public void testAirlines() throws Throwable {
    for( int i=0; i<10; i++ ) {
      new DRFTest().basicDRF(
        //
        //"../demo/c5/row10000.csv.gz", "c5.hex", null, null,

        "../datasets/UCI/UCI-large/covtype/covtype.data", "covtype.hex", null, null,
        new DRFTest.PrepData() { @Override int prep(Frame fr) { return fr.numCols()-1; } },
        10/*ntree*/,
        ar( ar( 199019,   7697,    15,    0,  180,    45,   546),
           ar(   8012, 267788,   514,    7,  586,   329,   181),
           ar(     16,    707, 33424,  162,   53,   639,     0),
           ar(      1,      5,   353, 2211,    0,    99,     0),
           ar(    181,   1456,   134,    0, 7455,    43,     4),
           ar(     30,    540,  1171,   96,   33, 15109,     0),
           ar(    865,    167,     0,    0,    9,     0, 19075)),
        ar("1", "2", "3", "4", "5", "6", "7"),

        //"./smalldata/iris/iris_wheader.csv", "iris.hex", null, null,
        //new DRFTest.PrepData() { @Override int prep(Frame fr) { return fr.numCols()-1; } },
        //10/*ntree*/,
        //a( a( 50,  0,  0),
        //   a(  0, 50,  0),
        //   a(  0,  0, 50)),
        //s("Iris-setosa","Iris-versicolor","Iris-virginica"),

        //"./smalldata/logreg/prostate.csv", "prostate.hex", null, null,
        //new DRFTest.PrepData() { @Override int prep(Frame fr) {
        //  UKV.remove(fr.remove("ID")._key); return fr.find("CAPSULE");
        //  } },
        //10/*ntree*/,
        //a( a(170, 55),
        //   a( 60, 92)),
        //s("0","1"),


        99/*max_depth*/,
        20/*nbins*/,
        0 /*optflag*/  );
    }
  }
  @Test @Ignore public void dummy_test() {
    /* this is just a dummy test to avoid JUnit complains about missing test */
  }

  @Override
  protected void testBalanceWithCrossValidation(String dataset, int response, int[] ignored_cols, int ntrees, int nfolds) {
    Frame f = parseFrame(dataset);
    DRFModel model = null;
    DRF drf = new DRF();
    try {
      Vec respVec = f.vec(response);
      // Build a model
      drf.source = f;
      drf.response = respVec;
      drf.ignored_cols = ignored_cols;
      drf.classification = true;
      drf.ntrees = ntrees;
      drf.seed = 42;
      drf.balance_classes = true;
      drf.n_folds = nfolds;
      drf.keep_cross_validation_splits = false;
      drf.invoke();
      Assert.assertEquals("Number of cross validation model is wrond!", nfolds, drf.xval_models.length);
      model = UKV.get(drf.dest());
      Assert.assertTrue(model.get_params().state == Job.JobState.DONE); //HEX-1817
    } finally {
      if (f!=null) f.delete();
      if (model!=null) {
        if (drf.xval_models!=null) {
          for (Key k : drf.xval_models) {
            Model m = UKV.get(k);
            m.delete();
          }
        }
        model.delete();
      }
    }
  }
}
