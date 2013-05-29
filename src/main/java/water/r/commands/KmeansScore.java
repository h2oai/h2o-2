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
public class KmeansScore implements Invokable {

  /** The name of the R command. */
  public String name() {
    return "h2o.kmeans.score";
  }

  /** Function called from R to perform the parse. */
  @Override public RAny invoke(ArgumentInfo ai, RAny[] args) {
    Arg arg = defaultArg();
    if( Interop.getAttributeAsString(ai.getAny(args, "data"), "h2okind") != "HEX" ) return Interop
        .asRString("Error wrong data argument");
    if( Interop.getAttributeAsString(ai.getAny(args, "model"), "h2okind") != "KMEANS.MODEL" ) return Interop
        .asRString("Error wrong model argument");
    arg.data = ai.get(args, "data", arg.data);
    arg.model = ai.get(args, "model", arg.model);
    arg.classify = ai.get(args, "classify", arg.classify);
    Res res = execute(arg);
    if( res.error == null ) {
      if( !arg.classify ) {
        RAny r = Interop.asRDoubleVector(res.result);
        return r;
      } else {
        RAny rds = Interop.asRString(res.destination);
        rds = Interop.setAttribute(rds, "h2okind", "HEX");
        return rds;
      }
    } else return Interop.asRString(res.error.toString());
  }

  private static final String[] params = new String[] { "data", "model", "classify" };

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
    /* model key name */
    String model;
    boolean classify = false;
  }

  /** Return a fresh argument objects with default values. */
  static public Arg defaultArg() {
    return new Arg();
  }

  /** Return value of the command. */
  static public class Res extends Arguments.Opt {
    /* The result of the parse; null if an error occurred */
    double[] result;
    /* Key for the class assignment of the rows. */
    String destination;
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
    KMeansModel model = DKV.get(Key.make(arg.model)).get();
    int colIds[] = model.columnMapping(va.colNames());
    if( !Model.isCompatible(colIds) ) for( int i = 0; i < colIds.length; i++ )
      if( colIds[i] == -1 ) throw new IllegalArgumentException("Incompatible dataset: " + va._key
          + " does not have column '" + model._va._cols[i]._name + "'");
    try {
      if( !arg.classify ) {
        KMeansModel.KMeansScore kms = KMeansModel.KMeansScore.score(model, va);
        res.result = kms._dist;
      } else {
        Key dest = Key.make(arg.data + ".mod"); // bad name... FIXME
        Job job = KMeansModel.KMeansApply.run(dest, model, va);
        job.get();
        res.destination = dest.toString();
      }
    } catch( Error e ) {
      res.error = e;
    }
    return res;
  }
}
