package water.util;

import java.io.*;
import java.lang.management.ManagementFactory;

import water.H2O;
import water.Timer;
import water.util.L.Tag.Sys;

/** To be renamed Log. Right now it causes ambiguities with the old Log class **/
abstract public class L {

	public static interface Tag {
		public static enum Sys implements Tag {
		RANDF, GENLM, KMEAN, PARSE, STORE, WATER;
		}
	  public static enum Kind implements Tag {
	    INFO,WARN,ERRR;
	  }
	}

	/** Verbosity for debug log level */
	static private volatile int level = Integer.getInteger("h2o.log.debug.level", 3);
  public static final String HOST= H2O.findInetAddressForSelf().getHostAddress();
  public static final long PID = getPid();
  private static final String HOST_AND_PID =  "" + fixedLength(HOST + " ", 13) + fixedLength(PID + " ", 6);


	static class Event{
	  Object where;
	  Tag kind;
	  Tag sys;
	  Timer when;
	  Throwable ouch;
	  Object[] message;

	  Event(Object where, Tag.Sys sys, Tag.Kind kind, Throwable ouch, Object[] message) {
	    this.where=where;this.kind=kind;
	    this.ouch=ouch;this.message=message;
	    this.sys=sys;
	    this.when=new Timer();
	  }
	  public String toString() {
	    StringBuffer buf = new StringBuffer(120);
	    buf.append(when.startAsString()).append(" ").append(HOST_AND_PID);
	    buf.append(kind.toString()).append(" ").append(sys.toString()).append(": ");
	    for(Object m :message) buf.append(m.toString());
	    if (buf.indexOf("\n")!=-1) {
	      String s = buf.toString();
	      String[] lines = s.split("\n");
	      StringBuffer buf2 = new StringBuffer(2*buf.length());
	      buf2.append(lines[0]);
	      for(int i=1;i<lines.length;i++) buf2.append("\n+   ").append(lines[i]);
	      buf=buf2;
	    }
	    if (ouch!=null) {
	      buf.append("\n");
	      Writer wr = new StringWriter();
	      PrintWriter pwr = new PrintWriter(wr);
	      ouch.printStackTrace(pwr);
	      String mess = wr.toString();
	      String[] lines = mess.split("\n");
	      for(int i=0;i<lines.length;i++) {
	        buf.append("+   ").append(lines[i]);
	        if (i!=lines.length-1) buf.append("\n");
	      }
	    }
	    return buf.toString();
	  }
	}
	static public Throwable err(Object _this, Tag t, String msg, Throwable exception) {
		// some printing
		return exception;
	}

	static public void warn(Object _this, Tag t, String msg) {

	}

	static public void info(Object _this, Tag.Sys t, Object... objects) {
	  Event e = new Event(_this,t, Tag.Kind.INFO,null,objects);
	  System.out.println(e.toString());
	}

	static public void debug(Object _this, Tag t, Object... objects) {

	}

	static public void debug1(Object _this, Tag t, Object... objects) {
		if (level < 1) return;

	}

	static public void debug2(Object _this, Tag t, Object... objects) {
		if (level < 2) return;
	}

	static public void debug3(Object _this, Tag t, Object... objects) {
		if (level < 3) return;

	}

	/** This is not guaranteed to be unique. **/
  private static long getPid() {
    try {
      String n = ManagementFactory.getRuntimeMXBean().getName(); // implementation specific name
      int i = n.indexOf('@');
      if( i == -1 ) return -1;
      return Long.parseLong(n.substring(0, i));
    } catch( Throwable t ) { return -1; }
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
    while( strb.length() < size )  if( strb.length() < size ) strb.append(' ');
    return strb.toString();
  }

	public static void  main(String[]_) {
	  info(null,Sys.WATER,"boom\nbang");
	}
}
