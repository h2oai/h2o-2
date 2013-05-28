package water.r.commands;

import java.io.IOException;

import r.builtins.CallFactory.ArgumentInfo;
import r.data.RAny;
import r.ifc.Interop;
import r.ifc.Interop.Invokable;
import water.*;
import water.api.Constants.Extensions;
import water.api.Inspect;
import water.parser.CsvParser;
import water.parser.ParseDataset;

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
    String data = Interop.asString(ai.getAny(args, "data"));
    // check that it is a parsed-file-key.
    Arg arg = defaultArg();
    int k = (int) ai.get(args, "k", -1);
    double epsilon = ai.get(args, "epsilon", 0);
    int maxIteration =  (int)ai.get(args, "max.iteration",1);
    int[] cols = null;
    ///ai.getAny(args, "cols");
    Key dest = Key.make(data.toString() + ".mod");
    ValueArray va = DKV.get(Key.make(data)).get();
    long seed = ai.get(args,"seed",1234567890);
    boolean normalize = ai.get(args,"normalize", true);
    try {
      hex.KMeans job = hex.KMeans.start(dest, va, k, epsilon, seed, normalize, cols);
      job.get();
      String ds = dest.toString();
      RAny rds = Interop.asRString(ds);
      rds = Interop.setAttribute(rds, "h2okind", "kmeans-model");
    } catch( IllegalArgumentException e ) {
    } catch( Error e ) {
    }
    return null;
  }

  private static final String[] params = new String[] { "data", "cols", "k", "epsilon", "seed", "normalize", "max.iterations" };

  /** List of required parameters */
  @Override public String[] requiredParameters() {
    return params;
  }

  /** List of all parameters. */
  @Override public String[] parameters() {
    return params;
  }

  /** Arguments passed to the command. */
  static public class Arg extends Arguments.Opt {
    /* List of file names to parse. */
    URI[] files = new URI[] {};
  }

  /** Return a fresh argument objects with default values. */
  static public Arg defaultArg() {
    return new Arg();
  }

  /** Return value of the command. */
  static public class Res extends Arguments.Opt {
    /* The result of the parse; null if an error occurred */
    ValueArray result;
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
    if( arg.files.length == 0 ) {
      res.error = new URI.FormatError("no files");
      return res;
    }
    Key[] ks = new Key[arg.files.length];
    for( int i = 0; i < ks.length; i++ ) {
      try {
        ks[i] = arg.files[i].get();
      } catch( IOException e ) {
        res.error = e;
        return res;
      }
    }
    Value v = DKV.get(ks[0]);
    byte separator = CsvParser.NO_SEPARATOR;
    CsvParser.Setup setup = Inspect.csvGuessValue(v, separator);
    if( setup._data == null || setup._data[0].length == 0 ) res.error = new IllegalArgumentException(
        "H2O cannot only handles common CSV formats: " + arg.files[0]);
    Key dest = Key.make(ks[0] + Extensions.HEX);
    try {
      Job job = ParseDataset.forkParseDataset(dest, ks, setup);
      res.result = job.get();
      for( Key k : ks )
        UKV.remove(k);
    } catch( Error e ) {
      res.error = e;
    }
    return res;
  }
}
