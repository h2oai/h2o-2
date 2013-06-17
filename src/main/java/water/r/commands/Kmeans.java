package water.r.commands;

import hex.KMeansModel;
import r.builtins.CallFactory.ArgumentInfo;
import r.data.RAny;
import r.ifc.Interop;
import r.ifc.Interop.Invokable;
import water.*;

/**
 * The R version of KMEANS.
 *
 */
public class Kmeans implements Invokable {

  /** The name of the R command. */
  public String name() {
    return "h2o.kmeans";
  }

  /** Function called from R to perform the parse. */
  @Override public RAny invoke(ArgumentInfo ai, RAny[] args) {
    Arg arg = defaultArg();
    if( Interop.getAttributeAsString(ai.getAny(args, "data"), "h2okind") != "HEX" ) return Interop
        .asRString("Error wrong data argument");
    arg.data = ai.get(args, "data", arg.data);
    arg.k = (int) ai.get(args, "k", arg.k);
    arg.epsilon = ai.get(args, "epsilon", (long) arg.epsilon);
    arg.maxIterations = (int) ai.get(args, "max.iterations", arg.maxIterations);
    arg.cols = Interop.asIntArray(ai.getAny(args, "cols"));
    arg.seed = ai.get(args, "seed", arg.seed);
    arg.normalize = ai.get(args, "normalize", arg.normalize);
    Res res = execute(arg);
    if( res.error == null ) {
      String ds = res.result._selfKey.toString();
      RAny rds = Interop.asRString(ds);
      rds = Interop.setAttribute(rds, "h2okind", "KMEANS.MODEL");
      return rds;
    } else return Interop.asRString(res.error.toString());
  }

  private static final String[] params = new String[] { "data", "cols", "k", "epsilon", "seed", "normalize",
      "max.iterations" };

  /** List of required parameters */
  @Override public String[] requiredParameters() {
    return new String[] { "data" };
  }

  /** List of all parameters. */
  @Override public String[] parameters() {
    return params;
  }

  /** Arguments passed to the command. */
  static public class Arg extends Arguments.Opt {
    /* Parsed data file key name */
    String data;
    /* Columns of the data to include in the Kmeans computation */
    int[] cols;
    int k = 2;
    double epsilon = 1e-4;
    long seed = 123456789012L;
    boolean normalize = false;
    int maxIterations = 10;
  }

  /** Return a fresh argument objects with default values. */
  static public Arg defaultArg() {
    return new Arg();
  }

  /** Return value of the command. */
  static public class Res extends Arguments.Opt {
    /* The result of the parse; null if an error occurred */
    KMeansModel result;
    /* An error recording why the parse failed. */
    Throwable error;
  }

  /** Return a fresh return value object. */
  static public Res defaultRes() {
    return new Res();
  }

  /**
   * This method does the work of the command. Exceptions related to the execution of the command
   * are returned in the result object.
   */
  Res execute(Arg arg) {
    Res res = defaultRes();
    ValueArray va = DKV.get(Key.make(arg.data)).get();
    if( arg.cols == null ) {
      arg.cols = new int[va._cols.length];
      for( int i = 0; i < arg.cols.length; i++ )
        arg.cols[i] = i;
    }
    try {
      Key dest = Key.make(arg.data + ".mod");
      hex.KMeans job = hex.KMeans.start(dest, va, arg.k, arg.epsilon, arg.seed, arg.normalize, arg.cols);
      job.get();
      res.result = DKV.get(dest).get();
    } catch( Error e ) {
      res.error = e;
    }
    return res;
  }
}
