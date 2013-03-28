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
  public static final String HOST;
  private static final String HOST_AND_PID;
  static boolean _dontDie;
  // @formatter:on

  static {
    HOST = H2O.findInetAddressForSelf().getHostAddress();
    HOST_AND_PID = "" + padRight(HOST + ", ", 17) + padRight(getPid() + ", ", 8);
  }

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

  private static final class Wrapper extends PrintStream {
    Wrapper(PrintStream parent) {
      super(parent);
    }

    static String h() {
      String h = _utcFormat.get().format(new Date()) + ", ";
      h += HOST_AND_PID;
      h += padRight(Thread.currentThread().getName() + ", ", 26);
      return h;
    }

    @Override
    public PrintStream printf(String format, Object... args) {
      return super.printf(h() + format, args);
    }

    @Override
    public PrintStream printf(Locale l, String format, Object... args) {
      return super.printf(l, h() + format, args);
    }

    @Override
    public void println(String x) {
      super.println(h() + x);
    }

    void printlnParent(String s) {
      super.println(s);
    }
  }
}
