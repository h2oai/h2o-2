package hex.drf;

import water.*;
import water.fvec.Frame;
import water.fvec.Vec;
import water.util.Log.Tag.Sys;
import water.util.Log;
import water.util.Utils;

// Class for running DRF from the cmd line
public class Runner {

  public static class OptArgs extends Arguments.Opt {
    String h2oArgs;             // Extra args for H2O
    int    clusterSize=1;       // Stall till cluster gets this big
    String trainFile = "smalldata/gbm_test/ecology_model.csv";
    String  testFile = "smalldata/gbm_test/ecology_eval.csv";
    String response  = "Angaus";
    String cols      = "SegSumT,SegTSeas,SegLowFlow,DSDist,DSMaxSlope,USAvgT,USRainDays,USSlope,USNative,DSDam,Method,LocSed";
    int    mtries;
    int    ntrees    = 50;
    int    depth     = 999;
    float  sample    = 0.6666667f;
    int    nbins     = 20;
    long   seed      = 0xae44a87f9edf1cbL;
  }

  public static void main(String[] args) {
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
    if( ARGS.nbins  <  2 || ARGS.nbins  > 100000 ) throw new RuntimeException("nbins " +ARGS.nbins +" out of bounds");
    if( ARGS.depth  <= 1 )                         throw new RuntimeException("depth " +ARGS.depth +" out of bounds");

    String cs[] = ARGS.cols.split("[, \t]");

    // Set mtries
    if( ARGS.mtries == 0 )  ARGS.mtries = (int)Math.sqrt(cs.length);
    if( ARGS.mtries <= 0 || ARGS.mtries >cs.length)throw new RuntimeException("mtries "+ARGS.mtries+" out of bounds");

    // Load data
    Timer t_start = new Timer();
    Frame train = TestUtil.parseFrame(Key.make("train.hex"),ARGS.trainFile);
    Frame test  = TestUtil.parseFrame(Key.make( "test.hex"),ARGS. testFile);
    Log.info(Sys.RANDF,"Data loaded in "+t_start);

    // Pull out the response vector from the train data
    Vec response = train.subframe(new String[] {ARGS.response} ).vecs()[0];

    // Build a Frame with just the requested columns.
    train = train.subframe(cs);
    test  =  test.subframe(cs);

    Log.info(Sys.RANDF,"Arguments used:\n"+ARGS.toString());
    System.out.println(train);
    System.out.println(test);
  }

}
