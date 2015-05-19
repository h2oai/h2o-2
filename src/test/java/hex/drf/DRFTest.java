package hex.drf;

import hex.drf.DRF.DRFModel;

import hex.gbm.GBM;
import org.junit.*;

import static org.junit.Assert.assertEquals;
import water.*;
import water.api.AUC;
import water.api.DRFModelView;
import water.fvec.Frame;
import water.fvec.RebalanceDataSet;
import water.fvec.Vec;
import water.util.Log;

public class DRFTest extends TestUtil {

  private final void testHTML(DRF.DRFModel m) {
    StringBuilder sb = new StringBuilder();
    DRFModelView drfv = new DRFModelView();
    drfv.drf_model = m;
    drfv.toHTML(sb);
    assert(sb.length() > 0);
  }

  @BeforeClass public static void stall() { stall_till_cloudsize(1); }

  abstract static class PrepData { abstract int prep(Frame fr); }

  static final String[] s(String...arr)  { return arr; }
  static final long[]   a(long ...arr)   { return arr; }
  static final long[][] a(long[] ...arr) { return arr; }

  @Test public void testClassIris1() throws Throwable {

    // iris ntree=1
    // the DRF should  use only subset of rows since it is using oob validation
    basicDRFTestOOBE(
          "./smalldata/iris/iris.csv","iris.hex",
          new PrepData() { @Override int prep(Frame fr) { return fr.numCols()-1; } },
          1,
          a( a(25, 0,  0),
             a(0, 17,  1),
             a(1, 2, 15)),
          s("Iris-setosa","Iris-versicolor","Iris-virginica") );

  }

  @Test public void testClassIris5() throws Throwable {
    // iris ntree=50
    basicDRFTestOOBE(
          "./smalldata/iris/iris.csv","iris.hex",
          new PrepData() { @Override int prep(Frame fr) { return fr.numCols()-1; } },
          5,
          a( a(41, 0,  0),
             a(0, 39,  3),
             a(0,  4, 41)),
          s("Iris-setosa","Iris-versicolor","Iris-virginica") );
  }

  @Test public void testClassCars1() throws Throwable {
    // cars ntree=1
    basicDRFTestOOBE(
        "./smalldata/cars.csv","cars.hex",
        new PrepData() { @Override int prep(Frame fr) { UKV.remove(fr.remove("name")._key); return fr.find("cylinders"); } },
        1,
        a( a(0,  0, 0, 0, 0),
           a(0, 62, 0, 7, 0),
           a(0,  1, 0, 0, 0),
           a(0,  0, 0,31, 0),
           a(0,  0, 0, 0,40)),
        s("3", "4", "5", "6", "8"));
  }

  @Test public void testClassCars5() throws Throwable {
    basicDRFTestOOBE(
        "./smalldata/cars.csv","cars.hex",
        new PrepData() { @Override int prep(Frame fr) { UKV.remove(fr.remove("name")._key); return fr.find("cylinders"); } },
        5,
        a( a(3,   0, 0,  0,   0),
           a(0, 173, 2,  9,   0),
           a(0,   1, 1,  0,   0),
           a(0,   2, 2, 68,   2),
           a(0,   0, 0,  2,  88)),
        s("3", "4", "5", "6", "8"));
  }

  @Test public void testConstantCols() throws Throwable {
    try {
      basicDRFTestOOBE(
        "./smalldata/poker/poker100","poker.hex",
        new PrepData() { @Override int prep(Frame fr) {
          for (int i=0; i<7;i++) UKV.remove(fr.remove(3)._key);
          return 3;
        } },
        1,
        null,
        null);
      Assert.fail();
    } catch( IllegalArgumentException iae ) { /*pass*/ }
  }

  @Ignore @Test public void testBadData() throws Throwable {
    basicDRFTestOOBE(
        "./smalldata/test/drf_infinitys.csv","infinitys.hex",
        new PrepData() { @Override int prep(Frame fr) { return fr.find("DateofBirth"); } },
        1,
        a( a(6, 0),
           a(9, 1)),
        s("0", "1"));
  }

  //@Test
  public void testCreditSample1() throws Throwable {
    basicDRFTestOOBE(
        "./smalldata/kaggle/creditsample-training.csv.gz","credit.hex",
        new PrepData() { @Override int prep(Frame fr) {
          UKV.remove(fr.remove("MonthlyIncome")._key); return fr.find("SeriousDlqin2yrs");
          } },
        1,
        a( a(46294, 202),
           a( 3187, 107)),
        s("0", "1"));

  }

  @Test
  public void testCreditProstate1() throws Throwable {
    basicDRFTestOOBE(
        "./smalldata/logreg/prostate.csv","prostate.hex",
        new PrepData() { @Override int prep(Frame fr) {
          UKV.remove(fr.remove("ID")._key); return fr.find("CAPSULE");
          } },
        1,
        a( a(62, 19),
           a(31, 22)),
        s("0", "1"));

  }


  @Test public void testAirlines() throws Throwable {
    basicDRFTestOOBE(
        "./smalldata/airlines/allyears2k_headers.zip","airlines.hex",
        new PrepData() {
          @Override int prep(Frame fr) {
            UKV.remove(fr.remove("DepTime")._key);
            UKV.remove(fr.remove("ArrTime")._key);
            UKV.remove(fr.remove("ActualElapsedTime")._key);
            UKV.remove(fr.remove("AirTime")._key);
            UKV.remove(fr.remove("ArrDelay")._key);
            UKV.remove(fr.remove("DepDelay")._key);
            UKV.remove(fr.remove("Cancelled")._key);
            UKV.remove(fr.remove("CancellationCode")._key);
            UKV.remove(fr.remove("CarrierDelay")._key);
            UKV.remove(fr.remove("WeatherDelay")._key);
            UKV.remove(fr.remove("NASDelay")._key);
            UKV.remove(fr.remove("SecurityDelay")._key);
            UKV.remove(fr.remove("LateAircraftDelay")._key);
            UKV.remove(fr.remove("IsArrDelayed")._key);
            return fr.find("IsDepDelayed"); }
        },
        50,
        a( a(13941, 6946),
           a( 5885,17206)),
        s("NO", "YES"));
  }



  // Put response as the last vector in the frame and return it.
  // Also fill DRF.
  static Vec unifyFrame(DRF drf, Frame fr, PrepData prep) {
    int idx = prep.prep(fr);
    if( idx < 0 ) { drf.classification = false; idx = ~idx; }
    String rname = fr._names[idx];
    drf.response = fr.vecs()[idx];
    fr.remove(idx);           // Move response to the end
    fr.add(rname,drf.response);
    return drf.response;
  }

  public void basicDRFTestOOBE(String fnametrain, String hexnametrain, PrepData prep, int ntree, long[][] expCM, String[] expRespDom) throws Throwable { basicDRF(fnametrain, hexnametrain, null, null, prep, ntree, expCM, expRespDom, 10/*max_depth*/, 20/*nbins*/, 0/*optflag*/); }
  public void basicDRF(String fnametrain, String hexnametrain, String fnametest, String hexnametest, PrepData prep, int ntree, long[][] expCM, String[] expRespDom, int max_depth, int nbins, int optflags) throws Throwable {
    DRF drf = new DRF();
    Key destTrain = Key.make(hexnametrain);
    Key destTest  = hexnametest!=null?Key.make(hexnametest):null;
    Frame frTest = null, pred = null;
    DRFModel model = null;
    try {
      Frame frTrain = drf.source = parseFrame(destTrain, fnametrain);
      unifyFrame(drf, frTrain, prep);
      // Configure DRF
      drf.classification = true;
      drf.ntrees = ntree;
      drf.max_depth = max_depth;
      drf.min_rows = 1; // = nodesize
      drf.nbins = nbins;
      drf.mtries = -1;
      drf.sample_rate = 0.66667f;   // Simulated sampling with replacement
      drf.seed = (1L<<32)|2;
      drf.destination_key = Key.make("DRF_model_4_" + hexnametrain);
      // Invoke DRF and block till the end
      drf.invoke();
      // Get the model
      model = UKV.get(drf.dest());
      Assert.assertTrue(model.get_params().state == Job.JobState.DONE); //HEX-1817
      testHTML(model);
      // And compare CMs
      assertCM(expCM, model.cms[model.cms.length-1]._arr);
      Assert.assertEquals("Number of trees differs!", ntree, model.errs.length-1);
      String[] cmDom = model._domains[model._domains.length-1];
      Assert.assertArrayEquals("CM domain differs!", expRespDom, cmDom);

      frTest = fnametest!=null ? parseFrame(destTest, fnametest) : null;
      pred = drf.score(frTest!=null?frTest:drf.source);

    } finally {
      drf.source.delete();
      UKV.remove(drf.response._key);
      drf.remove();
      if (frTest!=null) frTest.delete();
      if( model != null ) model.delete(); // Remove the model
      if( pred != null ) pred.delete();
    }
  }

  @Test public void testReproducibility() {
    Frame tfr=null;
    final int N = 5;
    double[] mses = new double[N];

    Scope.enter();
    try {
      // Load data, hack frames
      tfr = parseFrame(Key.make("air.hex"), "./smalldata/covtype/covtype.20k.data");

      // rebalance to 256 chunks
      Key dest = Key.make("df.rebalanced.hex");
      RebalanceDataSet rb = new RebalanceDataSet(tfr, dest, 256);
      H2O.submitTask(rb);
      rb.join();
      tfr.delete();
      tfr = DKV.get(dest).get();

      for (int i=0; i<N; ++i) {
        DRF parms = new DRF();
        parms.source = tfr;
        parms.response = tfr.lastVec();
        parms.nbins = 1000;
        parms.ntrees = 1;
        parms.max_depth = 8;
        parms.mtries = -1;
        parms.min_rows = 10;
        parms.classification = false;
        parms.seed = 1234;

        // Build a first model; all remaining models should be equal
        DRFModel drf = parms.fork().get();
        mses[i] = drf.mse();

        drf.delete();
      }
    } finally{
      if (tfr != null) tfr.delete();
    }
    Scope.exit();
    for (int i=0; i<mses.length; ++i) {
      Log.info("trial: " + i + " -> mse: " + mses[i]);
    }
    for (int i=0; i<mses.length; ++i) {
      assertEquals(mses[i], mses[0], 1e-15);
    }
  }

  public static class repro {
    @Ignore
    @Test public void testAirline() throws InterruptedException {
      Frame tfr=null;
      Frame test=null;

      Scope.enter();
      try {
        // Load data, hack frames
        tfr = parseFrame(Key.make("air.hex"), "/users/arno/sz_bench_data/train-1m.csv");
        test = parseFrame(Key.make("airt.hex"), "/users/arno/sz_bench_data/test.csv");
        for (int i : new int[]{0,1,2}) {
          tfr.vecs()[i] = tfr.vecs()[i].toEnum();
          test.vecs()[i] = test.vecs()[i].toEnum();
        }

        DRF parms = new DRF();
        parms.source = tfr;
        parms.validation = test;
//        parms.ignored_cols_by_name = new int[]{4,5,6};
//        parms.ignored_cols_by_name = new int[]{0,1,2,3,4,5,7};
        parms.response = tfr.lastVec();
        parms.nbins = 20;
        parms.ntrees = 100;
        parms.max_depth = 20;
        parms.mtries = -1;
        parms.sample_rate = 0.667f;
        parms.min_rows = 10;
        parms.classification = true;
        parms.seed = 12;

        DRFModel drf = parms.fork().get();
        Frame pred = drf.score(test);
        AUC auc = new AUC();
        auc.vactual = test.lastVec();
        auc.vpredict = pred.lastVec();
        auc.invoke();
        Log.info("Test set AUC: " + auc.data().AUC);
        drf.delete();
      } finally{
        if (tfr != null) tfr.delete();
        if (test != null) test.delete();
      }
      Scope.exit();
    }
  }
}
