package water.r.commands;

import hex.*;
import hex.DGLM.GLMModel;
import hex.DGLM.GLMParams;
import hex.DLSM.ADMMSolver;
import r.builtins.CallFactory.ArgumentInfo;
import r.data.RAny;
import r.ifc.Interop;
import r.ifc.Interop.Invokable;
import water.*;

/**
 * The R version of GLM.
 *
 */
public class RGLM implements Invokable {

  /** The name of the R command. */
  public String name() {
    return "h2o.glm";
  }

  @Override public RAny invoke(ArgumentInfo ai, RAny[] args) {
    Arg arg = defaultArg();
    if( Interop.getAttributeAsString(ai.getAny(args, "data"), "h2okind") != "HEX" ) return Interop
        .asRString("Error wrong data argument");
    arg.data = ai.get(args, "data", arg.data);
    arg.X = Interop.asIntArray(ai.getAny(args, "X"));
    arg.Y = Interop.asInteger(ai.getAny(args, "Y"));
    arg.family = ai.get(args, "family", arg.family);
    arg.normalize = ai.get(args, "normalize", arg.normalize);
    if(ai.provided("lambda")) arg.lambda = Interop.asDouble(ai.getAny(args, "lambda"));
    if(ai.provided("alpha")) arg.alpha = Interop.asDouble(ai.getAny(args, "alpha"));
    Res res = execute(arg);
    if( res.error == null ) {
      String ds = res.result._selfKey.toString();
      RAny rds = Interop.asRString(ds);
      rds = Interop.setAttribute(rds, "h2okind", "GLM.MODEL");
      rds = Interop.setAttribute(rds, "json", res.result.toJson().toString());
      return rds;
    } else return Interop.asRString(res.error.toString());
  }

  private static final String[] params = new String[] {"data", "Y","X", "betaStart","family", "alpha","lambda","normalize","xval"};

  /** List of required parameters */
  @Override public String[] requiredParameters() {
    return new String[] { "data","Y"};
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
    int[] X;
    int Y;
    double [] betaStart = null;
    double alpha = 0.5;
    double lambda = 1e-5;
    String family = "gaussian";
    String link = "identity";
    int xval = 10;
    boolean normalize = true;
  }

  /** Return a fresh argument objects with default values. */
  static public Arg defaultArg() {
    return new Arg();
  }

  /** Return value of the command. */
  static public class Res extends Arguments.Opt {
    /* The result of the parse; null if an error occurred */
    GLMModel result;
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
    if( arg.X == null ) {
      arg.X = new int[va._cols.length-1];
      int j = 0;
      for( int i = 0; i < va._cols.length; i++ )
        if(i != arg.Y)arg.X[j++] = i;
    }
    try {
      Key dest = Key.make(arg.data + ".mod");
      Job job = hex.DGLM.startGLMJob(dest, DGLM.getData(va, arg.X, arg.Y, null, arg.normalize), new ADMMSolver(arg.lambda,arg.alpha), new GLMParams(DGLM.Family.valueOf(arg.family)), arg.betaStart, arg.xval, true);
      job.get();
      res.result = DKV.get(dest).get();
    } catch( Error e ) {
      res.error = e;
    }
    return res;
  }
}
