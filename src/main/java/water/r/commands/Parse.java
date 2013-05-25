package water.r.commands;

import java.io.IOException;

import r.builtins.CallFactory.ArgumentInfo;
import r.data.RAny;
import r.ifc.Interop;
import r.ifc.Interop.Invokable;
import water.*;
import water.api.Inspect;
import water.api.Constants.Extensions;
import water.parser.CsvParser;
import water.parser.ParseDataset;

/**
 * The R version of parse.
 *
 * The parse command is currently blocking; in the future we will support a non-blocking version by
 * return a result object that may contain a future.
 *
 * The command does both import and parse.
 */
public class Parse implements Invokable {

  /** The name of the R command. */
  public String name() {
    return "h2o.parse";
  }

  /** Function called from R to perform the parse. */
  @Override public RAny invoke(ArgumentInfo ai, RAny[] args) {
    String[] files = Interop.asStringArray(ai.getAny(args, "files"));
    Arg arg = defaultArg();
    URI[] uris = new URI[files.length];
    for( int i = 0; i < files.length; i++ )
      uris[i] = URI.make(files[i]);
    arg.files = uris;
    Res res = execute(arg);
    return Interop.asRString((res.error == null) ? res.result._key.toString() : res.error.toString());
  }

  private static final String[] params = new String[] { "files" };

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
        "I cannot figure out this file; I only handle common CSV formats: " + arg.files[0]);
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
