package water;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public final class Log {
  // @formatter:off
  private static final ThreadLocal<SimpleDateFormat> _utcFormat = new ThreadLocal<SimpleDateFormat>() {
    @Override
    protected SimpleDateFormat initialValue() {
      SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd'-'HH:mm:ss.SSS");
      format.setTimeZone(TimeZone.getTimeZone("UTC"));
      return format;
    }
  };
  private static final String HOST_AND_PID;
  // @formatter:on

  static {
    HOST_AND_PID = ""
        + padRight(H2O.findInetAddressForSelf().getHostAddress() + ", ", 17)
        + padRight(getPid() + ", ", 8);
  }

  private static long getPid() {
    try {
      String n = ManagementFactory.getRuntimeMXBean().getName();
      int i = n.indexOf('@');
      if( i == -1 ) return -1;
      return Long.parseLong(n.substring(0,i));
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
    String header = _utcFormat.get().format(new Date()) + ", ";
    header += HOST_AND_PID;
    header += padRight(Thread.currentThread().getName() + ", ", 12);
    System.out.println(header + (s != null ? s + " " + stack : stack));
  }

  // Print to the original STDERR & die
  public static void die(String s) {
    System.err.println(s);
    System.exit(-1);
  }

  static String padRight(String stringToPad, int size) {
    StringBuilder strb = new StringBuilder(stringToPad);

    while( strb.length() < size )
      if( strb.length() < size )
        strb.append(' ');

    return strb.toString();
  }
}
