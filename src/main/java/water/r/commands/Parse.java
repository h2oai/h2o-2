package water.r.commands;

import java.io.IOException;

import r.builtins.CallFactory.ArgumentInfo;
import r.data.RAny;
import r.ifc.Interop.Invokable;
import water.*;
import water.api.Inspect;
import water.parser.CsvParser;
import water.parser.ParseDataset;

/**
 * The R version of parsing.
 *
 * The parse command is currently designed to be blocking and to do both import and parse at the
 * same time.
 */
public class Parse implements Invokable {
  public String name() {
    return "h2o.parse";
  }

  @Override public RAny invoke(ArgumentInfo ai, RAny[] args) {
    throw new RuntimeException("TODO Auto-generated method stub");
  }

  @Override public String[] requiredParameters() {
    throw new RuntimeException("TODO Auto-generated method stub");
  }

  @Override public String[] parameters() {
    throw new RuntimeException("TODO Auto-generated method stub");
  }

  static public class Arg extends Arguments.Opt {
    URI[] names = new URI[] {};
  }

  static public Arg defaultArg() {
    return new Arg();
  }

  static public class Res extends Arguments.Opt {
    /* The result of the parse; null if an error occured */
    ValueArray result;
    /* An error recording why the parse failed. */
    Throwable error;
  }

  static public Res defaultRes() {
    return new Res();
  }

  /**
   * The execute method does the work. Eventually it will have to take all the parse arguments. But
   * right now it is at its simplest.
   */
  Res execute(Arg arg) {
    Res res = defaultRes();
    if( arg.names.length == 0 ) {
      res.error = new URI.FormatError("no files");
      return res;
    }
    Key[] ks = new Key[arg.names.length];
    for( int i = 0; i < ks.length; i++ ) {
      try {
        ks[i] = arg.names[i].get().getKey();
      } catch( IOException e ) {
        res.error = e;
        return res;
      }
    }
    Value v = DKV.get(ks[0]);
    byte separator = CsvParser.NO_SEPARATOR;
    CsvParser.Setup setup = Inspect.csvGuessValue(v, separator);
    if( setup._data == null || setup._data[0].length == 0 ) res.error = new IllegalArgumentException(
        "I cannot figure out this file; I only handle common CSV formats: " + arg.names[0]);
    Key dest = Key.make(ks[0] + ".hex");
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
