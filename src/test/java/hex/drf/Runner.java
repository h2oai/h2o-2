package hex.drf;

import water.*;
import water.fvec.Frame;
import water.fvec.Vec;
import water.util.Log.Tag.Sys;
import water.util.Log;
import hex.gbm.SharedTreeModelBuilder;
import hex.gbm.GBM;

// Class for running DRF from the cmd line
// Run as : java -jar h2o.jar water.Boot -mainClass hex.drf.Runner <runner_args>
// Example: java -jar h2o.jar water.Boot -mainClass hex.drf.Runner -trainFile=smalldata/iris/iris_wheader.csv -testFile= -response=class -cols=sepal_len,sepal_wid,petal_len,petal_wid -ntrees=5 -mtries=3
public class Runner {

  // Every field in this class is also a command-line argument.
  public static class OptArgs extends Arguments.Opt {
    String h2oArgs;             // Extra args for H2O
    int    clusterSize=1;       // Stall till cluster gets this big
    final static String defaultTrainFile = "smalldata/gbm_test/ecology_model.csv";
    final static String defaultTestFile  = "smalldata/gbm_test/ecology_eval.csv";
    String trainFile = defaultTrainFile;
    String  testFile = defaultTestFile;
    String response  = "Angaus";
    String cols      = "SegSumT,SegTSeas,SegLowFlow,DSDist,DSMaxSlope,USAvgT,USRainDays,USSlope,USNative,DSDam,Method,LocSed";
    boolean regression=false;   // Defalt to classification (vs regression)
    int    min_rows  = 1;       // Smallest number of rows per terminal
    int    ntrees    = 10;      // Number of trees
    int    depth     = 999;     // Max tree depth
    int    nbins     = 20;      // Nominal bins per column histogram
    boolean gbm      = false;   // True for GBM, False for DRF
    int    mtries    = 0;       // Number of columns to try; zero defaults to Sqrt
    float  sample    = 0.6666667f; // Sampling rate
    long   seed      = 0xae44a87f9edf1cbL;
    float  learn     = 0.1f;    //
    float  splitTestTrain = Float.NaN; // Ratio on test/train split
  }

  public static void main(String[] args) throws Throwable {
    OptArgs ARGS = new Arguments(args).extract(new OptArgs());

    // Bring up the cluster
    String[] h2oArgs;
    String as = ARGS.h2oArgs;
    if( as != null ) {
      if( as.startsWith("\"") && as.endsWith("\"") ) as = as.substring(1, as.length()-1);
      h2oArgs = as.trim().split("[ \t]+");
    } else h2oArgs=new String[0];
    H2O.main(h2oArgs);

    // Make sure we shutdown on all exit paths
    try {
      main(ARGS);
    } catch( Throwable t ) {
      t.printStackTrace();
      throw t;
    } finally {
      UDPRebooted.T.shutdown.broadcast();
    }
  }

  // Do the Work
  static void main(OptArgs ARGS) {
    // Finish building the cluster
    TestUtil.stall_till_cloudsize(ARGS.clusterSize);

    // Sanity check basic args
    if( ARGS.ntrees <= 0 || ARGS.ntrees > 100000 ) throw new RuntimeException("ntrees "+ARGS.ntrees+" out of bounds");
    if( ARGS.sample <  0 || ARGS.sample > 1.0f   ) throw new RuntimeException("sample "+ARGS.sample+" out of bounds");
    if( ARGS.learn  <  0 || ARGS.learn  > 1.0f   ) throw new RuntimeException("learn " +ARGS.learn +" out of bounds");
    if( ARGS.nbins  <  2 || ARGS.nbins  > 100000 ) throw new RuntimeException("nbins " +ARGS.nbins +" out of bounds");
    if( ARGS.depth  <= 0 )                         throw new RuntimeException("depth " +ARGS.depth +" out of bounds");
    if( ARGS.splitTestTrain < 0 || ARGS.splitTestTrain > 1.0f ) throw new RuntimeException("splitTestTrain "+ARGS.splitTestTrain+" out of bounds");
    // If trainFile is NOT set, you are doing the default file and cannot set testFile.
    if( (ARGS.trainFile == OptArgs.defaultTrainFile) && (ARGS.testFile != OptArgs.defaultTestFile) )
      throw new RuntimeException("Cannot set test file unless also setting train file");
    // If testFile is set, cannot set splitTestTrain
    if( (ARGS.testFile != OptArgs.defaultTestFile) && !Float.isNaN(ARGS.splitTestTrain) )
      throw new RuntimeException("Cannot have both testFile and splitTestTrain");

    Sys sys = ARGS.gbm ? Sys.GBM__ : Sys.DRF__;

    String cs[] = (ARGS.cols+","+ARGS.response).split("[,\t]");

    // Set mtries
    if( ARGS.mtries == 0 )  ARGS.mtries = (int)Math.sqrt(cs.length);
    if( ARGS.mtries <= 0 || ARGS.mtries >cs.length)throw new RuntimeException("mtries "+ARGS.mtries+" out of bounds");

    // Load data
    Timer t_load = new Timer();
    Key trainkey = Key.make("train.hex");
    Key  testkey = Key.make( "test.hex");
    Frame train = TestUtil.parseFrame(trainkey,ARGS.trainFile);
    Frame test = null;
    if( !Float.isNaN(ARGS.splitTestTrain) ) {
      water.exec.Exec2.exec("r=runif(train.hex,-1); test.hex=train.hex[r>=0.7,]; train.hex=train.hex[r<0.7,]").remove_and_unlock();
      train = UKV.get(trainkey);
      test  = UKV.get( testkey);
    } else if( ARGS.testFile.length() != 0 ) {
      test = TestUtil.parseFrame(testkey,ARGS. testFile);
    }
    Log.info(sys,"Data loaded in "+t_load);

    // Pull out the response vector from the train data
    Vec response = train.subframe(new String[] {ARGS.response}).vecs()[0];

    // Build a Frame with just the requested columns.
    train = train.subframe(cs);
    if( test != null ) test = test.subframe(cs);
    Vec vs[] = train.vecs();
    for( Vec v : vs ) v.min(); // Do rollups
    for( int i=0; i<train.numCols(); i++ )
      Log.info(sys,train._names[i]+", "+vs[i].min()+" - "+vs[i].max()+(vs[i].naCnt()==0?"":(", missing="+vs[i].naCnt())));

    Log.info(sys,"Arguments used:\n"+ARGS.toString());
    Timer t_model = new Timer();
    SharedTreeModelBuilder stmb = ARGS.gbm ? new GBM() : new DRF();
    stmb.source = train;
    stmb.validation = test;
    stmb.classification = !ARGS.regression;
    stmb.response   = response;
    stmb.ntrees     = ARGS.ntrees;
    stmb.max_depth  = ARGS.depth;
    stmb.min_rows   = ARGS.min_rows;
    stmb.destination_key = Key.make("DRF_Model_" + ARGS.trainFile);
    if( ARGS.gbm ) {
      GBM gbm = (GBM)stmb;
      gbm.learn_rate = ARGS.learn;
    } else {
      DRF drf = (DRF)stmb;
      drf.mtries     = ARGS.mtries;
      drf.sample_rate= ARGS.sample;
      drf.seed       = ARGS.seed;
    }
    // Invoke DRF and block till the end
    stmb.invoke();
    Log.info(sys,"Model trained in "+t_model);
  }

}
