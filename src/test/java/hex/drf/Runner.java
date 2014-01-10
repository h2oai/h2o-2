package hex.drf;

import water.*;
import water.fvec.Frame;
import water.fvec.Vec;
import water.util.Log.Tag.Sys;
import water.util.Log;
import water.util.Utils;
import hex.gbm.SharedTreeModelBuilder;
import hex.gbm.GBM;

// Class for running DRF from the cmd line
public class Runner {

  public static class OptArgs extends Arguments.Opt {
    String h2oArgs;             // Extra args for H2O
    int    clusterSize=1;       // Stall till cluster gets this big
    final static String defaultTrainFile = "smalldata/gbm_test/ecology_model.csv";
    final static String defaultTestFile  = "smalldata/gbm_test/ecology_eval.csv";
    String trainFile = defaultTrainFile;
    String  testFile = defaultTestFile;
    String response  = "Angaus";
    String cols      = "SegSumT,SegTSeas,SegLowFlow,DSDist,DSMaxSlope,USAvgT,USRainDays,USSlope,USNative,DSDam,Method,LocSed";
    int    min_rows  = 1;       // Smallest number of rows per terminal
    int    ntrees    = 10;      // Number of trees
    int    depth     = 999;     // Max tree depth
    int    nbins     = 20;      // Nominal bins per column histogram
    boolean gbm      = false;   // True for GBM, False for DRF
    int    mtries    = 0;       // Number of columns to try; zero defaults to Sqrt
    float  sample    = 0.6666667f; // Sampling rate
    long   seed      = 0xae44a87f9edf1cbL;
    float  learn     = 0.1f;    // 
  }

  public static void main(String[] args) throws Throwable {
    OptArgs ARGS = new Arguments(args).extract(new OptArgs());

    // Bring up the cluster
    String[] h2oArgs;
    String as = ARGS.h2oArgs;
    if( as == null ) {       // By default run using local IP, C.f. JUnitRunner
      h2oArgs = new String[] { "-ip=127.0.0.1", "-flatfile=" + Utils.writeFile("127.0.0.1:54321").getAbsolutePath() };
    } else {
      if( as.startsWith("\"") && as.endsWith("\"") ) as = as.substring(1, as.length()-1);
      h2oArgs = as.trim().split("[ \t]+");
    }
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
    if( ARGS.learn  <  0 || ARGS.learn  > 1.0f   ) throw new RuntimeException("learn  "+ARGS.learn +" out of bounds");
    if( ARGS.nbins  <  2 || ARGS.nbins  > 100000 ) throw new RuntimeException("nbins " +ARGS.nbins +" out of bounds");
    if( ARGS.depth  <= 1 )                         throw new RuntimeException("depth " +ARGS.depth +" out of bounds");
    if( (ARGS.trainFile != OptArgs.defaultTrainFile) ^ (ARGS.testFile != OptArgs.defaultTestFile) )
      throw new RuntimeException("Set both trainFile and testFile; a missing testFile will use OOBEE on train data");
    Sys sys = ARGS.gbm ? Sys.GBM__ : Sys.DRF__;

    String cs[] = (ARGS.cols+","+ARGS.response).split("[, \t]");

    // Set mtries
    if( ARGS.mtries == 0 )  ARGS.mtries = (int)Math.sqrt(cs.length);
    if( ARGS.mtries <= 0 || ARGS.mtries >cs.length)throw new RuntimeException("mtries "+ARGS.mtries+" out of bounds");

    // Load data
    Timer t_load = new Timer();
    Frame train = TestUtil.parseFrame(Key.make("train.hex"),ARGS.trainFile);
    Frame test  = ARGS.testFile.length()==0 ? null : TestUtil.parseFrame(Key.make("test.hex"),ARGS. testFile);
    Log.info(sys,"Data loaded in "+t_load);

    // Pull out the response vector from the train data
    Vec response = train.subframe(new String[] {ARGS.response} ).vecs()[0];

    // Build a Frame with just the requested columns.
    train = train.subframe(cs);
    if( test != null ) test = test.subframe(cs);
    for( Vec v : train.vecs() ) v.min(); // Do rollups
    for( int i=0; i<train.numCols(); i++ ) 
      Log.info(sys,train._names[i]+", "+train.vecs()[i]);

    Log.info(sys,"Arguments used:\n"+ARGS.toString());
    Timer t_model = new Timer();
    SharedTreeModelBuilder stmb = ARGS.gbm ? new GBM() : new DRF();
    stmb.source = train;
    stmb.validation = test;
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
