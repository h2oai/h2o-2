package water;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.text.SimpleDateFormat;
import java.util.*;

public final class Log {
  // @formatter:off
  private static final ThreadLocal<SimpleDateFormat> _utcFormat = new ThreadLocal<SimpleDateFormat>() {
    @Override
    protected SimpleDateFormat initialValue() {
      SimpleDateFormat format = new SimpleDateFormat("yyMMdd'-'HH:mm:ss.SSS");
      format.setTimeZone(TimeZone.getTimeZone("UTC"));
      return format;
    }
  };
  // Survive "die" calls - used in some debugging modes
  static boolean _dontDie;
  // @formatter:on

  // Pre-cooked ip/name/pid string
  public static final String HOST;
  private static final String HOST_AND_PID;
  private static final long PID;
  static {
    HOST = H2O.findInetAddressForSelf().getHostAddress();
    PID=getPid();
    HOST_AND_PID = "" + padRight(HOST + ", ", 17) + padRight(PID + ", ", 8);
  }
  private static final String NL = System.getProperty("line.separator");

  // Local (not-distributed) log file
  private static BufferedWriter LOG_FILE;
  public final static Key LOG_KEY = Key.make("Log",(byte)0,Key.BUILT_IN_KEY);

  private static long getPid() {
    try {
      String n = ManagementFactory.getRuntimeMXBean().getName();
      int i = n.indexOf('@');
      if( i == -1 )
        return -1;
      return Long.parseLong(n.substring(0, i));
    } catch( Throwable t ) {
      return -1;
    }
  }

  public static void write(String s) {
    write(s, null);
  }

  public static void write(Throwable t) {
    write(null, t);
  }

  public static void write(String s, Throwable t) {
    String stack = "";
    if( t != null ) {
      Writer result = new StringWriter();
      PrintWriter printWriter = new PrintWriter(result);
      t.printStackTrace(printWriter);
      stack = result.toString();
    }
    System.out.println(s != null ? s + " " + stack : stack);
  }

  // Print to the original STDERR & die
  public static void die(String s) {
    System.err.println(s);
    if( !_dontDie )
      System.exit(-1);
  }

  public static String padRight(String stringToPad, int size) {
    StringBuilder strb = new StringBuilder(stringToPad);

    while( strb.length() < size )
      if( strb.length() < size )
        strb.append(' ');

    return strb.toString();
  }

  public static void initHeaders() {
    if( !(System.out instanceof Wrapper) ) {
      System.setOut(new Wrapper(System.out));
      System.setErr(new Wrapper(System.err));
    }
  }

  public static void unwrap(PrintStream stream, String s) {
    if( stream instanceof Wrapper )
      ((Wrapper) System.out).printlnParent(s);
    else
      stream.println(s);
  }

  public static void log(File file, PrintStream stream) throws Exception {
    BufferedReader reader = new BufferedReader(new FileReader(file));
    try {
      for( ;; ) {
        String line = reader.readLine();
        if( line == null )
          break;
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
      String date = _utcFormat.get().format(new Date());
      sb.append(date).append(", ");
      sb.append(HOST_AND_PID);
      String thr = Thread.currentThread().getName();
      sb.append(thr);
      int len = thr.length();
      for( int i=len; i<26; i++ )
        sb.append(' ');
      String msg = String.format(l,format,args);
      sb.append(msg);
      if( nl ) sb.append(NL);
      String s = sb.toString();
      // Write to 3 places: stderr/stdout, the local log file, the K/V store
      // Open/create the logfile when we can
      if( H2O.SELF != null && LOG_FILE == null )
        LOG_FILE = new BufferedWriter(PersistIce.logFile());
      // Write to the log file
      if( LOG_FILE != null ) {
        try { LOG_FILE.write(s,0,s.length()); LOG_FILE.flush(); }
        catch( IOException ioe ) {/*ignore log-write fails*/}
      }
      if( Paxos._cloudLocked ) logToKV(date,thr,msg);
      // Returned string goes to stderr/stdout
      return s;
    }

    private static void logToKV( final String date, final String thr, final String msg ) {
      final long pid = PID;     // Run locally
      final H2ONode h2o = H2O.SELF; // Run locally
      new TAtomic<LogStr>() {
        @Override public LogStr atomic( LogStr l ) {
          return new LogStr(l,date,h2o,pid,thr,msg);
        }
      }.fork(LOG_KEY);
    }

    @Override
    public PrintStream printf(String format, Object... args) {
      super.print(log(null,false,format,args));
      return this;
    }

    @Override
    public PrintStream printf(Locale l, String format, Object... args) {
      super.print(log(l,false,format, args));
      return this;
    }

    @Override
    public void println(String x) {
      super.print(log(null,true,"%s",x));
    }

    void printlnParent(String s) {
      super.println(s);
    }
  }

  // Class to hold a ring buffer of log messages in the K/V store
  public static class LogStr extends Iced {
    public static final int MAX = 1024; // Number of log entries
    public final int _idx;      // Index into the ring buffer
    public final String  _dates[];
    public final H2ONode _h2os [];
    public final long    _pids [];
    public final String  _thrs [];
    public final String  _msgs [];
    LogStr( LogStr l, String date, H2ONode h2o, long pid, String thr, String msg ) { 
      _dates= l==null ? new String [MAX] : l._dates;
      _h2os = l==null ? new H2ONode[MAX] : l._h2os ;
      _pids = l==null ? new long   [MAX] : l._pids ;
      _thrs = l==null ? new String [MAX] : l._thrs ;
      _msgs = l==null ? new String [MAX] : l._msgs ;
      _idx =  l==null ? 0                : (l._idx+1)&(MAX-1);
      _dates[_idx] = date;
      _h2os [_idx] = h2o;
      _pids [_idx] = pid;
      _thrs [_idx] = thr;
      _msgs [_idx] = msg;
    }
  }
}
