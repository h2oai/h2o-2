package water.util;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.util.Locale;

import water.*;
import water.util.Log.Tag.Kind;
import water.util.Log.Tag.Sys;

/** Log for H2O. This class should be loaded before we start to print as it wraps around
 * System.{out,err}.
 *
 *  There are three kinds of message:  INFO, WARN and ERRR, for general information,
 *  events that look wrong, and runtime exceptions.
 *  WARN messages and uncaught exceptions are printed on Standard output. Some
 *  INFO messages are also printed on standard output.  Many more messages are
 *  printed to the log file in the ice directory and to the K/V store.
 *
 *  Messages can come from a number of subsystems, Sys.RANDF for instance
 *  denotes the Random forest implementation. Subsystem names are five letter
 *  mnemonics to keep formatting nicely even.
 *
 *  To print messages from a subsystem to the log file, set a property on the command line
 *     -Dlog.RANDF=true
 *     -Dlog.RANDF=false    // turn off
 *  or call the API function
 *     Log.setFlag(Sys.RANDF);
 *     Log.unsetFlag(Sys.RANDF);   // turn off
 *
 *
 *  OOME: when the VM is low on memory, OutOfMemoryError can be thrown in the
 *  logging framework while it is trying to print a message. In this case the
 *  first message that fails is recorded for later printout, and a number of
 *  messages can be discarded. The framework will attempt to print the recorded
 *  message later, and report the number of dropped messages, but this done in
 *  a best effort and lossy manner. Basically when an OOME occurs during
 *  logging, no guarantees are made about the messages.
 **/
abstract public class Log {

  /** Tags for log messages */
  public static interface Tag {
    /** Which subsystem of h2o? */
    public static enum Sys implements Tag {
      RANDF, GENLM, KMEAN, PARSE, STORE, WATER, HDFS_, HTTPD, CLEAN, CONFM, EXCEL, SCORM;
      boolean _enable;
    }

    /** What kind of message? */
    public static enum Kind implements Tag {
      INFO, WARN, ERRR;
    }
  }
  static {
    for(Kind k : Kind.values())
      assert k.name().length() == Kind.INFO.name().length();
    for(Sys s : Sys.values())
      assert s.name().length() == Sys.RANDF.name().length();
  }
  public static final Kind[] KINDS = Kind.values();
  public static final Sys[] SYSS = Sys.values();

  private static final String NL = System.getProperty("line.separator");
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
  private static boolean printAll;
  /** Per subsystem debugging flags. */
  static {
    String pa = System.getProperty("log.printAll");
    printAll = (pa!=null && pa.equals("true"));

    setFlag(Sys.WATER);
    setFlag(Sys.RANDF);
    setFlag(Sys.HTTPD);
    for(Sys s : Sys.values()) {
      String str = System.getProperty("log."+s);
      if (str == null) continue;
      if (str.equals("false")) unsetFlag(s); else setFlag(s);
    }
  }

  /** Check if a subsystem will print debug message to the LOG file */
  public static boolean flag(Sys t) { return t._enable || printAll; }
  /** Set the debug flag. */
  public static void setFlag(Sys t) { t._enable = true; }
  /** Unset the debug flag. */
  public static void unsetFlag(Sys t) { t._enable = false; }

  /**
   * Events are created for all calls to the logging API.
   **/
  static class Event {
    Kind kind;
    Sys sys;
    Timer when;
    long msFromStart;
    Throwable ouch;
    Object[] messages;
    Object message;
    String thread;
    /**True if we have yet finished printing this event.*/
    volatile boolean printMe;

    private volatile static Timer lastGoodTimer = new Timer();
    private volatile static Event lastEvent = new Event();
    private volatile static int missed;

    static Event make(Tag.Sys sys, Tag.Kind kind, Throwable ouch, Object[] messages) {
      return make0(sys, kind, ouch, messages , null);
    }
    static Event make(Tag.Sys sys, Tag.Kind kind, Throwable ouch, Object message) {
      return make0(sys, kind, ouch, null , message);
    }
    static private Event make0(Tag.Sys sys, Tag.Kind kind, Throwable ouch, Object[] messages, Object message) {
      Event result = null;
      try {
        result = new Event();
        result.init(sys, kind, ouch, messages, message, lastGoodTimer = new Timer());
      } catch (OutOfMemoryError e){
        synchronized (Event.class){
          if (lastEvent.printMe) {  missed++;  return null; }// Giving up; record the number of lost messages
          result = lastEvent;
          result.init(sys, kind, ouch, messages, null, lastGoodTimer);
        }
      }
      return result;
    }

    private void init(Tag.Sys sys, Tag.Kind kind, Throwable ouch, Object[] messages, Object message, Timer t) {
      this.kind = kind;
      this.ouch = ouch;
      this.messages = messages;
      this.message = message;
      this.sys = sys;
      this.when = t;
      this.printMe = true;
    }

    public String toString() {
      StringBuilder buf = longHeader(new StringBuilder(120));
      int headroom = buf.length();
      buf.append(body(headroom));
      return buf.toString();
    }

    public String toShortString() {
      StringBuilder buf = shortHeader(new StringBuilder(120));
      int headroom = buf.length();
      buf.append(body(headroom));
      return buf.toString();
    }

    private String body(int headroom) {
      //if( body != null ) return body; // the different message have different padding ... can't quite cache.
      StringBuilder buf = new StringBuilder(120);
      if (messages!=null) for( Object m : messages )  buf.append(m.toString());
      else if (message !=null ) buf.append(message.toString());
      // --- "\n" vs NL ---
      // Embedded strings often use "\n" to denote a new-line.  This is either
      // 1 or 2 chars ON OUTPUT depending Unix vs Windows, but always 1 char in
      // the incoming string.  We search & split the incoming string based on
      // the 1 character "\n", but we build result strings with NL (a String of
      // length 1 or 2).  i.e.
      // GOOD: String.indexOf("\n"); SB.append( NL )
      // BAD : String.indexOf( NL ); SB.append("\n")
      if( buf.indexOf("\n") != -1 ) {
        String s = buf.toString();
        String[] lines = s.split("\n");
        StringBuilder buf2 = new StringBuilder(2 * buf.length());
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
        String[] lines = mess.split("\n");
        for( int i = 0; i < lines.length; i++ ) {
          buf.append("+");
          for( int j = 1; j < headroom; j++ )
            buf.append(" ");
          buf.append(lines[i]);
          if( i != lines.length - 1 ) buf.append(NL);
        }
      }
      return buf.toString();
    }

    private StringBuilder longHeader(StringBuilder buf) {
      buf.append(when.startAsString()).append(" ").append(HOST_AND_PID);
      if( thread == null ) thread = fixedLength(Thread.currentThread().getName() + " ", 10);
      buf.append(thread);
      buf.append(kind.toString()).append(" ").append(sys.toString()).append(": ");
      return buf;
    }

    private StringBuilder shortHeader(StringBuilder buf) {
      buf.append(when.startAsShortString()).append(" ");
      if( thread == null ) thread = fixedLength(Thread.currentThread().getName() + " ", 8);
      buf.append(thread);
      buf.append(kind.toString()).append(" ").append(sys.toString()).append(": ");
      return buf;
    }
  }


  /** Write different versions of E to the three outputs. */
  private static void write(Event e, boolean printOnOut) {
    try {
      write0(e,printOnOut);
      if (Event.lastEvent.printMe || Event.missed > 0) {
        synchronized(Event.class){
          if ( Event.lastEvent.printMe) {
            Event ev = Event.lastEvent;
            write0(ev,true);
            Event.lastEvent = new Event();
          }
          if (Event.missed > 0) {
            if (Event.lastEvent.printMe==false) {
              Event.lastEvent.init(Sys.WATER, Kind.WARN, null, null, "Logging framework dropped a message", Event.lastGoodTimer);
              Event.missed--;
            }
          }
        }
      }
    } catch (OutOfMemoryError _) {
      synchronized (Event.class){
        if (Event.lastEvent.printMe == false)
         Event.lastEvent = e;
        else Event.missed++;
      }
    }
  }
  /** the actual write code. */
  private static void write0(Event e, boolean printOnOut) {
    String s = e.toString();
    if( H2O.SELF != null && LOG_FILE == null ) LOG_FILE = new BufferedWriter(PersistIce.logFile());
    if( LOG_FILE != null ) {
      try {
        LOG_FILE.write(s, 0, s.length());
        LOG_FILE.write(NL,0,NL.length());
        LOG_FILE.flush();
      } catch( IOException ioe ) {/* ignore log-write fails */
      }
    }
    if( Paxos._cloudLocked ) logToKV(e.when.startAsString(), e.thread, e.kind, e.sys, e.body(0));
    if(printOnOut || printAll) unwrap(System.out,e.toShortString());
    e.printMe = false;
  }
  /** We also log events to the store. */
  private static void logToKV(final String date, final String thr, final Kind kind, final Sys sys, final String msg) {
    final long pid = PID; // Run locally
    final H2ONode h2o = H2O.SELF; // Run locally
    new TAtomic<LogStr>() {
      @Override public LogStr atomic(LogStr l) {
        return new LogStr(l, date, h2o, pid, thr, kind, sys, msg);
      }
    }.fork(LOG_KEY);
  }
  /** Record an exception to the log file and store. */
  static public <T extends Throwable> T err(Sys t, String msg, T exception) {
    Event e =  Event.make(t, Kind.ERRR, exception, msg );
    write(e,true);
    return exception;
  }
  /** Record an exception to the log file and store. */
  static public <T extends Throwable> T err(String msg, T exception) {
    return err(Sys.WATER, msg, exception);
  }
  /** Record an exception to the log file and store. */
  static public <T extends Throwable> T err(Sys t, T exception) {
    return err(t, "", exception);
  }
  /** Record an exception to the log file and store. */
  static public <T extends Throwable> T err(T exception) {
    return err(Sys.WATER, "", exception);
  }
  /** Record an exception to the log file and store and return a new
   * RuntimeException that wraps around the exception. */
  static public RuntimeException errRTExcept(Throwable exception) {
    return new RuntimeException(err(Sys.WATER, "", exception));
  }
  /** Log a warning to standard out, the log file and the store. */
  static public <T extends Throwable> T warn(Sys t, String msg, T exception) {
    Event e =  Event.make(t, Kind.WARN, exception,  msg);
    write(e,true);
    return exception;
  }
  /** Log a warning to standard out, the log file and the store. */
  static public Throwable warn(Sys t, String msg) {
    return warn(t, msg, null);
  }
  /** Log a warning to standard out, the log file and the store. */
  static public Throwable warn(String msg) {
    return warn(Sys.WATER, msg, null);
  }
  /** Log an information message to standard out, the log file and the store. */
  static public void info(Sys t, Object... objects) {
    Event e =  Event.make(t, Kind.INFO, null, objects);
    write(e,true);
  }
  /** Log an information message to standard out, the log file and the store. */
  static public void info(Object... objects) {
    info(Sys.WATER, objects);
  }
  /** Log a debug message to the log file and the store if the subsystem's flag is set. */
  static public void debug(Object... objects) {
    if (flag(Sys.WATER) == false) return;
    Event e =  Event.make(Sys.WATER, Kind.INFO, null, objects);
    write(e,false);
  }
  /** Log a debug message to the log file and the store if the subsystem's flag is set. */
  static public void debug(Sys t, Object... objects) {
    if (flag(t) == false) return;
    Event e =  Event.make( t, Kind.INFO, null, objects);
    write(e,false);
  }
  /** Log a debug message to the log file and the store if the subsystem's flag is set. */
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

  /** Print a message to the stream without the logging information. */
  public static void unwrap(PrintStream stream, String s) {
    if( stream instanceof Wrapper ) ((Wrapper) stream).printlnParent(s);
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
      String msg = String.format(l, format, args);
      Event e =  Event.make(Sys.WATER,Kind.INFO,null, msg);
      Log.write(e,false);
      return e.toShortString()+NL;
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
    public final byte _kinds[];
    public final byte _syss[];
    public final String _dates[];
    public final H2ONode _h2os[];
    public final long _pids[];
    public final String _thrs[];
    public final String _msgs[];

    LogStr(LogStr l, String date, H2ONode h2o, long pid, String thr, Kind kind, Sys sys, String msg) {
      _dates = l == null ? new String[MAX] : l._dates;
      _h2os = l == null ? new H2ONode[MAX] : l._h2os;
      _pids = l == null ? new long[MAX] : l._pids;
      _thrs = l == null ? new String[MAX] : l._thrs;
      _kinds = l == null ? new byte[MAX] : l._kinds;
      _syss = l == null ? new byte[MAX] : l._syss;
      _msgs = l == null ? new String[MAX] : l._msgs;
      _idx = l == null ? 0 : (l._idx + 1) & (MAX - 1);
      _dates[_idx] = date;
      _h2os[_idx] = h2o;
      _pids[_idx] = pid;
      _thrs[_idx] = thr;
      _kinds[_idx] = (byte) kind.ordinal();
      _syss[_idx] = (byte) sys.ordinal();
      _msgs[_idx] = msg;
    }
  }

  public static void main(String[]args) {
      Log.info("hi");
      Log.info("h","i");
      unwrap(System.out,"hi");
      unwrap(System.err,"hi");

      Log.info("ho ",new Object(){
        int i;
        public String toString() { if (i++ ==0) throw new OutOfMemoryError(); else return super.toString(); } } );
      Log.info("ha ",new Object(){
        int i;
        public String toString() { if (i++ ==0) throw new OutOfMemoryError(); else return super.toString(); } } );
      Log.info("hi");
      Log.info("hi");
      Log.info("hi");

  }
}
