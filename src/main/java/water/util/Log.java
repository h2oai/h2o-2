package water.util;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.text.SimpleDateFormat;
import java.util.*;

import water.*;
import water.Timer;
import water.util.Log.Tag.Kind;
import water.util.Log.Tag.Sys;

/** Log for H2O. This class should be loaded early.
 *
 *  There are three kinds of message:  INFO, WARN and ERRR, for general information,
 *  events that look wrong, and runtime exceptions.
 *
 *  Messages can come from a number of subsystems.
 *
 *  There is a global debug level and a per subsystem debug level. They can be queried
 *  and set by the level()/setLevel() methods.
 **/
abstract public class Log {

  /** Tags for log messages */
  public static interface Tag {
    /** Which subsystem of h2o? */
    public static enum Sys implements Tag {
      RANDF, GENLM, KMEAN, PARSE, STORE, WATER, HDFS_, HTTPD, CLEAN, CONFM, EXCEL, SCOREM;
    }

    /** What kind of message? */
    public static enum Kind implements Tag {
      INFO, WARN, ERRR;
    }
  }

  private static final String NL = System.getProperty("line.separator");
  private static final PrintStream out = System.out;
  static {
    System.setOut(new Wrapper(System.out));
    System.setErr(new Wrapper(System.err));
  }
  /** Local log file */
  private static BufferedWriter LOG_FILE;
  /** Key for the log in the KV store */
  public final static Key LOG_KEY = Key.make("Log", (byte) 0, Key.BUILT_IN_KEY);
  /** Time from when this class loaded. */
  static final Timer time = new Timer();
  /** IP address of the host */
  public static final String HOST = H2O.findInetAddressForSelf().getHostAddress();
  /** Some guess at the process ID. */
  public static final long PID = getPid();
  /** Hostname and process ID. */
  private static final String HOST_AND_PID = "" + fixedLength(HOST + " ", 13) + fixedLength(PID + " ", 6);
  /** Debug level, higher means more output. */
  private static int level = Integer.getInteger("h2o.log.debug.level", 1);
  /** Per subsystem debug levels, higher means more output. */
  private static HashMap<Sys, Integer> levels = new HashMap<Sys, Integer>();
  static {
    for( Sys t : Sys.values() )
      levels.put(t, -1);
  }

  /** Return the current debugging level. It is OK for applications to change it. */
  public static int level() {
    return level;
  }

  /** Get the debug level for the subsystem, -1 if not set. */
  public static int level(Sys t) {
    return levels.get(t);
  }

  /** Set the debug level. */
  public static void setLevel(int i) {
    level = i;
  }

  /** We can set the debug level on a per-subsystem granularity. Returns -1 if not set. */
  public static void setLevel(Sys t, int i) {
    levels.put(t, i);
  }

  /**
   * Events are created for all calls to the logging API. In the current implementation we don't do
   * anything with them but this may change.
   **/
  static class Event {
    Kind kind;
    Sys sys;
    Timer when;
    long msFromStart;
    Throwable ouch;
    Object[] message;
    String thread;
    String body;

    Event(Tag.Sys sys, Tag.Kind kind, Throwable ouch, Object[] message) {
      this.kind = kind;
      this.ouch = ouch;
      this.message = message;
      this.sys = sys;
      this.when = new Timer();
    }

    public String toString() {
      StringBuffer buf = longHeader(new StringBuffer(120));
      int headroom = buf.length();
      buf.append(body(headroom));
      return buf.toString();
    }

    public String toShortString() {
      StringBuffer buf = shortHeader(new StringBuffer(120));
      int headroom = buf.length();
      buf.append(body(headroom));
      return buf.toString();
    }

    private String body(int headroom) {
      //if( body != null ) return body; // the different message have different padding ... can't quite cache.
      StringBuffer buf = new StringBuffer(120);
      for( Object m : message )
        buf.append(m.toString());
      if( buf.indexOf(NL) != -1 ) {
        String s = buf.toString();
        String[] lines = s.split(NL);
        StringBuffer buf2 = new StringBuffer(2 * buf.length());
        buf2.append(lines[0]);
        for( int i = 1; i < lines.length; i++ ) {
          buf2.append(NL).append("+");
          for( int j = 1; j < headroom; j++ )
            buf2.append(" ");
          buf2.append(lines[i]);
        }
        buf = buf2;
      }
      if( ouch != null ) {
        buf.append(NL);
        Writer wr = new StringWriter();
        PrintWriter pwr = new PrintWriter(wr);
        ouch.printStackTrace(pwr);
        String mess = wr.toString();
        String[] lines = mess.split(NL);
        for( int i = 0; i < lines.length; i++ ) {
          buf.append("+");
          for( int j = 1; j < headroom; j++ )
            buf.append(" ");
          buf.append(lines[i]);
          if( i != lines.length - 1 ) buf.append(NL);
        }
      }
      return body = buf.toString();
    }

    private StringBuffer longHeader(StringBuffer buf) {
      buf.append(when.startAsString()).append(" ").append(HOST_AND_PID);
      if( thread == null ) thread = fixedLength(Thread.currentThread().getName() + " ", 10);
      buf.append(thread);
      buf.append(kind.toString()).append(" ").append(sys.toString()).append(": ");
      return buf;
    }

    private StringBuffer shortHeader(StringBuffer buf) {
      buf.append(when.startAsShortString()).append(" ");
      if( thread == null ) thread = fixedLength(Thread.currentThread().getName() + " ", 8);
      buf.append(thread);
      buf.append(kind.toString()).append(" ").append(sys.toString()).append(": ");
      return buf;
    }
  }


  /** Write different versions of E to the three outputs. */
  private static void write(Event e, boolean printOnOut) {
    String s = e.toString();
    if( H2O.SELF != null && LOG_FILE == null ) LOG_FILE = new BufferedWriter(PersistIce.logFile());
    if( LOG_FILE != null ) {
      try {
        LOG_FILE.write(s, 0, s.length());
        LOG_FILE.flush();
      } catch( IOException ioe ) {/* ignore log-write fails */
      }
    }
    if( Paxos._cloudLocked ) logToKV(e.when.startAsString(), e.thread, e.toString());
    if(printOnOut) out.println(e.toShortString());
  }

  private static void logToKV(final String date, final String thr, final String msg) {
    final long pid = PID; // Run locally
    final H2ONode h2o = H2O.SELF; // Run locally
    new TAtomic<LogStr>() {
      @Override public LogStr atomic(LogStr l) {
        return new LogStr(l, date, h2o, pid, thr, msg);
      }
    }.fork(LOG_KEY);
  }

  static public <T extends Throwable> T err(Sys t, String msg, T exception) {
    Event e = new Event(t, Kind.ERRR, exception, new Object[] { msg });
    write(e,false);
    return exception;
  }

  static public <T extends Throwable> T err(String msg, T exception) {
    return err(Sys.WATER, msg, exception);
  }

  static public <T extends Throwable> T err(Sys t, T exception) {
    return err(t, "", exception);
  }

  static public <T extends Throwable> T err(T exception) {
    return err(Sys.WATER, "", exception);
  }

  static public RuntimeException errRTExcept(Throwable exception) {
    return new RuntimeException(err(Sys.WATER, "", exception));
  }

  static public <T extends Throwable> T err(String m) {
    return err(Sys.WATER, m, null);
  }

  static public <T extends Throwable> T warn(Sys t, String msg, T exception) {
    Event e = new Event(t, Kind.WARN, exception, new Object[] { msg });
    write(e,true);
    return exception;
  }

  static public Throwable warn(Sys t, String msg) {
    return warn(t, msg, null);
  }

  static public Throwable warn(String msg) {
    return warn(Sys.WATER, msg, null);
  }

  static public void info(Sys t, Object... objects) {
    Event e = new Event(t, Kind.INFO, null, objects);
    write(e,true);
  }

  static public void info(Object... objects) {
    info(Sys.WATER, objects);
  }

  static public void debug( Sys t, Object... objects) {
    Event e = new Event(t, Kind.INFO, null, objects);
    write(e,false);
  }

  static public void debug(  Object... objects) {
    Event e = new Event(Sys.WATER, Kind.INFO, null, objects);
    write(e,false);
  }


  static public void debug1(Sys t, Object... objects) {
    if( level() >= 1 || level(t) >= 1 ) return;
    Event e = new Event( t, Kind.INFO, null, objects);
    write(e,false);
  }

  static public void debug2( Sys t, Object... objects) {
    if( level() >= 2 || level(t) >= 2 ) return;
    Event e = new Event( t, Kind.INFO, null, objects);
    write(e,false);
  }

  static public void debug2(Object... objects) {
    debug(Sys.WATER, objects);
  }

  static public void debug3( Sys t, Object... objects) {
    if( level() >= 3 || level(t) >= 3 ) return;
    Event e = new Event( t, Kind.INFO, null, objects);
    write(e,false);
  }

  private static String fixedLength(String s, int length) {
    String r = padRight(s, length);
    if( r.length() > length ) {
      int a = Math.max(r.length() - length + 1, 0);
      int b = Math.max(a, r.length());
      r = "#" + r.substring(a, b);
    }
    return r;
  }

  public static String padRight(String stringToPad, int size) {
    StringBuilder strb = new StringBuilder(stringToPad);
    while( strb.length() < size )
      if( strb.length() < size ) strb.append(' ');
    return strb.toString();
  }

  /// ==== FROM OLD LOG ====

  private static final ThreadLocal<SimpleDateFormat> _utcFormat = new ThreadLocal<SimpleDateFormat>() {
    @Override protected SimpleDateFormat initialValue() {
      SimpleDateFormat format = new SimpleDateFormat("dd' 'HH:mm:ss.SSS");
      format.setTimeZone(TimeZone.getTimeZone("UTC"));
      return format;
    }
  };
  // Survive "die" calls - used in some debugging modes
  public static boolean _dontDie;

  // Return process ID, or -1 if not supported
  private static long getPid() {
    try {
      String n = ManagementFactory.getRuntimeMXBean().getName();
      int i = n.indexOf('@');
      if( i == -1 ) return -1;
      return Long.parseLong(n.substring(0, i));
    } catch( Throwable t ) {
      return -1;
    }
  }

  // Print to the original STDERR & die
  public static void die(String s) {
    System.err.println(s);
    if( !_dontDie ) System.exit(-1);
  }

  /** No op. */
  public static void initHeaders() {}

  public static void unwrap(PrintStream stream, String s) {
    if( stream instanceof Wrapper ) ((Wrapper) System.out).printlnParent(s);
    else stream.println(s);
  }

  public static void log(File file, PrintStream stream) throws Exception {
    BufferedReader reader = new BufferedReader(new FileReader(file));
    try {
      for( ;; ) {
        String line = reader.readLine();
        if( line == null ) break;
        stream.println(line);
      }
    } finally {
      reader.close();
    }
  }

  private static final class Wrapper extends PrintStream {
    Wrapper(PrintStream parent) {
      super(parent);
    }

    private static String log(Locale l, boolean nl, String format, Object... args) {
      // Build the String to be logged, with all sorts of headers
      StringBuilder sb = new StringBuilder();
      String thr = fixedLength(Thread.currentThread().getName() + " ", 15);
      String date = _utcFormat.get().format(new Date());
      /*
       * sb.append(date).append(" "); sb.append(HOST_AND_PID); sb.append(thr);
       */
      String msg = String.format(l, format, args);
      sb.append(msg);
      if( nl ) sb.append(NL);
      String s = sb.toString();
      // Write to 3 places: stderr/stdout, the local log file, the K/V store
      // Open/create the logfile when we can
      if( H2O.SELF != null && LOG_FILE == null ) LOG_FILE = new BufferedWriter(PersistIce.logFile());
      // Write to the log file
      if( LOG_FILE != null ) {
        try {
          LOG_FILE.write(s, 0, s.length());
          LOG_FILE.flush();
        } catch( IOException ioe ) {/* ignore log-write fails */
        }
      }
      if( Paxos._cloudLocked ) logToKV(date, thr, msg);
      // Returned string goes to stderr/stdout
      return s;
    }

    @Override public PrintStream printf(String format, Object... args) {
      super.print(log(null, false, format, args));
      return this;
    }

    @Override public PrintStream printf(Locale l, String format, Object... args) {
      super.print(log(l, false, format, args));
      return this;
    }

    @Override public void println(String x) {
      super.print(log(null, true, "%s", x));
    }

    void printlnParent(String s) {
      super.println(s);
    }
  }

  // Class to hold a ring buffer of log messages in the K/V store
  public static class LogStr extends Iced {
    public static final int MAX = 1024; // Number of log entries
    public final int _idx; // Index into the ring buffer
    public final String _dates[];
    public final H2ONode _h2os[];
    public final long _pids[];
    public final String _thrs[];
    public final String _msgs[];

    LogStr(LogStr l, String date, H2ONode h2o, long pid, String thr, String msg) {
      _dates = l == null ? new String[MAX] : l._dates;
      _h2os = l == null ? new H2ONode[MAX] : l._h2os;
      _pids = l == null ? new long[MAX] : l._pids;
      _thrs = l == null ? new String[MAX] : l._thrs;
      _msgs = l == null ? new String[MAX] : l._msgs;
      _idx = l == null ? 0 : (l._idx + 1) & (MAX - 1);
      _dates[_idx] = date;
      _h2os[_idx] = h2o;
      _pids[_idx] = pid;
      _thrs[_idx] = thr;
      _msgs[_idx] = msg;
    }
  }
}
