package water.util;

import java.io.*;
import java.lang.management.ManagementFactory;

import water.H2O;
import water.Timer;
import water.util.L.Tag.Kind;
import water.util.L.Tag.Sys;

/** To be renamed Log. Right now it causes ambiguities with the old Log class **/
abstract public class L {

  /** Tags for log messages */
  public static interface Tag {
    /** Which subsystem of h2o? */
    public static enum Sys implements Tag {
      RANDF, GENLM, KMEAN, PARSE, STORE, WATER, HDFS_, HTTPD, CLEANR;
    }

    /** What kind of message? */
    public static enum Kind implements Tag {
      INFO, WARN, ERRR;
    }
  }

  /** Verbosity for debug log level */
  static private volatile int level = Integer.getInteger("h2o.log.debug.level", 3);
  /** IP address of the host */
  public static final String HOST = H2O.findInetAddressForSelf().getHostAddress();
  /** Some guess at the process ID. */
  public static final long PID = getPid();
  /** Hostname and process ID. */
  private static final String HOST_AND_PID = "" + fixedLength(HOST + " ", 13) + fixedLength(PID + " ", 6);

  /**
   * Events are created for all calls to the logging API. In the current implementation we don't do
   * anything with them but this may change.
   **/
  static class Event {
    Object where;
    Tag kind;
    Tag sys;
    Timer when;
    Throwable ouch;
    Object[] message;

    Event(Object where, Tag.Sys sys, Tag.Kind kind, Throwable ouch, Object[] message) {
      this.where = where;
      this.kind = kind;
      this.ouch = ouch;
      this.message = message;
      this.sys = sys;
      this.when = new Timer();
    }

    public String toString() {
      StringBuffer buf = new StringBuffer(120);
      buf.append(when.startAsString()).append(" ").append(HOST_AND_PID);
      buf.append(kind.toString()).append(" ").append(sys.toString()).append(": ");
      int headroom = buf.length();
      for( Object m : message )
        buf.append(m.toString());
      if( buf.indexOf("\n") != -1 ) {
        String s = buf.toString();
        String[] lines = s.split("\n");
        StringBuffer buf2 = new StringBuffer(2 * buf.length());
        buf2.append(lines[0]);
        if( where != null )
          buf2.append(" (at ").append(classOf(where)).append(")");
        for( int i = 1; i < lines.length; i++ ) {
          buf2.append("\n+");
          for( int j = 1; j < headroom; j++ )
            buf2.append(" ");
          buf2.append(lines[i]);
        }
        buf = buf2;
      } else if( where != null )
        buf.append(" (at ").append(classOf(where)).append(")");
      if( ouch != null ) {
        buf.append("\n");
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
          if( i != lines.length - 1 )
            buf.append("\n");
        }
      }
      return buf.toString();
    }
  }

  public static String classOf(Object o) {
    String s = (o instanceof Class) ? o.toString() : o.getClass().toString();
    if( s.indexOf(" ") != -1 ) {
      s = s.split(" ")[1];
    }
    return s;
  }

  static public  <T extends Throwable> T err(Object _this, Sys t, String msg, T exception) {
    Event e = new Event(_this, t, Kind.ERRR, exception, new Object[] { msg });
    System.out.println(e.toString());
    return exception;
  }

  static public <T extends Throwable> T  err(T exception) {
    return err(null, Sys.WATER, "", exception);
  }

  static public <T extends Throwable> T warn(Object _this, Sys t, String msg, T exception) {
    Event e = new Event(_this, t, Kind.WARN, exception, new Object[] { msg });
    System.out.println(e.toString());
    return exception;
  }

  static public Throwable warn(Object _this, Sys t, String msg) {
    return warn(_this, t, msg, null);
  }

  static public Throwable warn(Sys t, String msg) {
    return warn(null, t, msg, null);
  }

  static public Throwable warn(String msg) {
    return warn(null, Sys.WATER, msg, null);
  }

  static public void info(Object _this, Sys t, Object... objects) {
    Event e = new Event(_this, t, Kind.INFO, null, objects);
    System.out.println(e.toString());
  }

  static public void info(Sys t, Object... objects) {
    info(null, t, objects);
  }

  static public void info(Object... objects) {
    info(null, Sys.WATER, objects);
  }

  static public void debug(Object _this, Sys t, Object... objects) {
    Event e = new Event(_this, t, Kind.INFO, null, objects);
    System.out.println(e.toString());
  }

  static public void debug(Sys t, Object... objects) {
    debug(null, t, objects);
  }

  static public void debug1(Object _this, Sys t, Object... objects) {
    if( level < 1 ) return;
    Event e = new Event(_this, t, Kind.INFO, null, objects);
    System.out.println(e.toString());
  }

  static public void debug2(Object _this, Sys t, Object... objects) {
    if( level < 2 ) return;
    Event e = new Event(_this, t, Kind.INFO, null, objects);
    System.out.println(e.toString());
  }
  static public void debug2(Object... objects) {
    debug(null,Sys.WATER,objects);
  }

  static public void debug3(Object _this, Sys t, Object... objects) {
    if( level < 3 ) return;
    Event e = new Event(_this, t, Kind.INFO, null, objects);
    System.out.println(e.toString());
  }

  /** This is not guaranteed to be unique. **/
  private static long getPid() {
    try {
      String n = ManagementFactory.getRuntimeMXBean().getName(); // implementation specific name
      int i = n.indexOf('@');
      if( i == -1 )
        return -1;
      return Long.parseLong(n.substring(0, i));
    } catch( Throwable t ) {
      return -1;
    }
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

  private static String padRight(String stringToPad, int size) {
    StringBuilder strb = new StringBuilder(stringToPad);
    while( strb.length() < size )
      if( strb.length() < size )
        strb.append(' ');
    return strb.toString();
  }

  public static void main(String[] _) {
    info(Sys.WATER, "boom\nbang");
    err(L.class, Sys.WATER, "too bad", new Error("boom"));
  }
}
