package water.parser;

import water.*;
import water.parser.DParseTask.Pass;

public class ParseStatus extends Iced {
  public DParseTask.Pass _phase;
  public long _sofar;
  public long _total;
  public String _error;

  public ParseStatus() { }
  public ParseStatus(long total) {
    _phase = Pass.ONE;
    _total = total;
  }
  public ParseStatus(String err) {
    _error = err;
  }

  public double getProgress() {
    return (_sofar/(double)_total + _phase.ordinal() ) / Pass.values().length;
  }

  public static void initialize(Key statusKey, long steps) {
    UKV.put(statusKey, new ParseStatus(steps));
  }

  public static void error(Key statusKey, String msg) {
    UKV.put(statusKey, new ParseStatus(msg));
  }

  public static void update(Key statusKey, final long steps, final Pass phase) {
    new TAtomic<ParseStatus>() {
      @Override
      public ParseStatus atomic(ParseStatus old) {
        if( old._phase == phase) {
          old._sofar += steps;
        } else if( old._phase==null || old._phase.ordinal() < phase.ordinal() ) {
          old._phase = phase;
          old._sofar = steps;
        } else {
          return null; // we are passed this phase already
        }
        return old;
      }
      @Override public ParseStatus alloc() { return new ParseStatus(); }
    }.invoke(statusKey);
  }
}
