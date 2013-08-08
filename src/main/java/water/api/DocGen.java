package water.api;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import water.*;
import water.util.Log;
import water.api.Request.*;
import java.util.Properties;

/**
 * Auto-gen doc support, for JSON & REST API docs
 * @author <a href="mailto:cliffc@0xdata.com"></a>
 */
public abstract class DocGen {
  static final HTML HTML = new HTML();
  static final ReST ReST = new ReST();

  public static void createFile (String fileName, String content) {
    try
    {
      FileWriter fstream = new FileWriter(fileName, false); //true tells to append data.
      BufferedWriter out = new BufferedWriter(fstream);
      out.write(content);
      out.close();
    }
    catch (Exception e)
    {
      System.err.println("Error: " + e.getMessage());
    }
  }

  public static void createReSTFilesInCwd() {
    createFile("ImportFiles.rst", new ImportFiles().ReSTHelp());
    createFile("ImportFiles2.rst", new ImportFiles2().ReSTHelp());
    createFile("Parse2.rst", new Parse2().ReSTHelp());
  }

  public static void main(String[] args) {
    H2O.main(args);

    waitForCloudSize(1, 10000);
    createReSTFilesInCwd();
    H2O.exit(0);
  }

  static void waitForCloudSize(int size, int ms) {
    H2O.waitForCloudSize(size, ms);
    Job.putEmptyJobList();
  }

  // Class describing meta-info about H2O queries and results.
  public static class FieldDoc {
    final String _name;           // Field name
    final String _help;           // Some descriptive text
    final int _min_ver, _max_ver; // Min/Max supported-version numbers
    final Class _clazz; // Java type: subtypes of Argument are inputs, otherwise outputs
    RequestArguments.Argument _arg; // Lazily filled in, as docs are asked for.
    public FieldDoc( String name, String help, int min, int max, Class C ) {
      _name = name; _help = help; _min_ver = min; _max_ver = max; _clazz = C;
    }
    @Override public String toString() {
      return "{"+_name+", from "+_min_ver+" to "+_max_ver+", "+_clazz.getSimpleName()+", "+_help+"}";
    }

    public final String version() {
      return "  From version "+_min_ver+
        (_max_ver==Integer.MAX_VALUE?" onward":" to version "+_max_ver);
    }

    public final boolean isArg () {
      return RequestArguments.Argument.class.isAssignableFrom(_clazz);
    }
    public final boolean isJSON(){ return !isArg(); }

    // Specific accessors for input arguments.  Not valid for JSON output fields.
    public RequestArguments.Argument arg(Request R) {
      if( _arg != null ) return _arg;
      Class clzz = R.getClass();
      // An amazing crazy API from the JDK again.  Cannot search for protected
      // fields without either (1) throwing NoSuchFieldException if you ask in
      // a subclass, or (2) sorting through the list of ALL fields and EACH
      // level of the hierarchy.  Sadly, I catch NSFE & loop.
      while( true ) {
        try {
          Object o = clzz.getDeclaredField(_name).get(R);
          return _arg=((RequestArguments.Argument)o);
        }
        catch(   NoSuchFieldException ie ) { clzz = clzz.getSuperclass(); }
        catch( IllegalAccessException ie ) { ie.printStackTrace(); return null; }
      }
    }

    // Get the queryDescription results for this field, as it appears in the
    // existing Request object.  This can vary for different Request R objects
    // for the same field type.  E.g. the GLM Int field 'XVAL' (cross-
    // validation count) has a min/max of 0 to 1000000, with a default of 10,
    // while the Plot Int field '_width' has a default of 800, and the PutValue
    // Int field '_rf' has a min/max of 0 to 255 (and default of 2).
    public final String argHelp( Request R ) { return arg(R).queryDescription(); }
    public final boolean required( Request R ) { return arg(R)._required; }
    public final String[] errors( Request R ) { return arg(R).errors(); }
    String argErr( ) { return "Argument '"+_name+"' error: "; }
  }

  // --------------------------------------------------------------------------
  // Abstract text generators, for building pretty docs in either HTML or
  // ReStructuredText form.
  abstract StringBuilder escape( StringBuilder sb, String s );
  abstract StringBuilder bodyHead( StringBuilder sb );
  abstract StringBuilder bodyTail( StringBuilder sb );
  abstract StringBuilder title( StringBuilder sb, String t );
  abstract StringBuilder section( StringBuilder sb, String t );
  abstract StringBuilder listHead( StringBuilder sb );
  abstract StringBuilder listBullet( StringBuilder sb, String s, String body, int d );
  abstract StringBuilder listTail( StringBuilder sb );
  abstract String bold( String s );
  abstract StringBuilder paraHead( StringBuilder sb );
  abstract StringBuilder paraTail( StringBuilder sb );
  StringBuilder paragraph( StringBuilder sb, String s ) {
    return paraTail(paraHead(sb).append(s));
  }

  public String genHelp(Request R) {
    final String name = R.getClass().getSimpleName();
    final FieldDoc docs[] = R.toDocField();
    final StringBuilder sb = new StringBuilder();
    bodyHead(sb);
    title(sb,name);
    paragraph(sb,"");

    section(sb,"Supported HTTP methods and descriptions");
    String gs = R.toDocGET();
    if( gs != null ) {
      paragraph(sb,"GET");
      paragraph(sb,gs);
    }

    section(sb,"URL");
    paraTail(escape(paraHead(sb),"http://<h2oHost>:<h2oApiPort>/"+name+".json"));

    // Escape out for not-yet-converted auto-doc Requests
    if( docs == null ) return bodyTail(sb).toString();

    section(sb,"Input parameters");
    listHead(sb);
    for( FieldDoc doc : docs )
      if( doc.isArg() ) {
        listBullet(sb,
                   bold(doc._name)+", "+doc.argHelp(R),
                   doc._help+doc.version(),0);
        String[] errors = doc.errors(R);
        if( errors != null || doc.required(R) ) {
          paragraph(sb,"");
          paragraph(sb,bold("Possible JSON error field returns:"));
          listHead(sb);
          if( errors != null )
            for( String err : errors )
              listBullet(sb,doc.argErr()+err,"",1);
          if( doc.required(R) )
            listBullet(sb,doc.argErr()+"Argument '"+doc._name+"' is required, but not specified","",1);
          listTail(sb);
        }
      }
    listTail(sb);

    section(sb,"Output JSON elements");
    listJSONFields(sb,docs);

    section(sb,"HTTP response codes");
    paragraph(sb,"200 OK");
    paragraph(sb,"Success and error responses are identical.");

    String s[] = R.DocExampleSucc();
    if( s != null ) {
      section(sb,"Success Example");
      paraHead(sb);
      url(sb,name,s);
      paraTail(sb);
      paragraph(sb,serve(name,s));
    }

    String f[] = R.DocExampleFail();
    if( f != null ) {
      section(sb,"Error Example");
      paraHead(sb);
      url(sb,name,f);
      paraTail(sb);
      paragraph(sb,serve(name,f));
    }

    bodyTail(sb);
    return sb.toString();
  }

  private void listJSONFields( StringBuilder sb, FieldDoc[] docs ) {
    listHead(sb);
    for( FieldDoc doc : docs )
      if( doc.isJSON() ) {
        listBullet(sb,
                   bold(doc._name)+", a "+doc._clazz.getSimpleName(),
                   doc._help+doc.version(),0);
        Class c = doc._clazz.getComponentType();
        if( c==null ) c = doc._clazz;
        if( Iced.class.isAssignableFrom(c) ) {
          try {
            FieldDoc[] nested = ((Iced)c.newInstance()).toDocField();
            listJSONFields(sb,nested);
          }
          catch( InstantiationException ie ) { water.util.Log.errRTExcept(ie); }
          catch( IllegalAccessException ie ) { water.util.Log.errRTExcept(ie); }
        }
      }
    listTail(sb);
  }

  private static StringBuilder url( StringBuilder sb, String name, String[] parms ) {
    sb.append("curl -s ").append(name).append(".json");
    boolean first = true;
    for( int i=0; i<parms.length; i+= 2 ) {
      if( first ) { first = false; sb.append("?"); }
      else        {                sb.append("&"); }
      sb.append(parms[i]).append('=').append(parms[i+1]);
    }
    return sb.append('\n');
  }

  private static String serve( String name, String[] parms ) {
    Properties p = new Properties();
    for( int i=0; i<parms.length; i+= 2 )
      p.setProperty(parms[i],parms[i+1]);
    NanoHTTPD.Response r = RequestServer.SERVER.serve(name+".json",null,null,p);
    try {
      int l = r.data.available();
      byte[] b = new byte[l];
      r.data.read(b);
      return new String(b);
    } catch( IOException ioe ) {
      Log.err(ioe);
      return null;
    }
  }

  // --------------------------------------------------------------------------
  // HTML flavored help text
  static class HTML extends DocGen {
    @Override StringBuilder escape(StringBuilder sb, String s ) {
      int len=s.length();
      for( int i=0; i<len; i++ ) {
        char c = s.charAt(i);
        if( false ) ;
        else if( c=='<' ) sb.append("&lt;");
        else if( c=='>' ) sb.append("&gt;");
        else if( c=='&' ) sb.append("&amp;");
        else if( c=='"' ) sb.append("&quot;");
        else sb.append(c);
      }
      return sb;
    }
    @Override StringBuilder bodyHead( StringBuilder sb ) {
      return sb.append("<div class='container'>"+
                       "<div class='row-fluid'>"+
                       "<div class='span12'>");
    }
    @Override StringBuilder bodyTail( StringBuilder sb ) { return sb.append("</div></div></div>"); }
    @Override StringBuilder title  ( StringBuilder sb, String t ) { return sb.append("<h3>").append(t).append("</h3>\n"); }
    @Override StringBuilder section( StringBuilder sb, String t ) { return sb.append("<h4>").append(t).append("</h4>\n"); }
    @Override StringBuilder paraHead( StringBuilder sb ) { return sb.append("<p>"); }
    @Override StringBuilder paraTail( StringBuilder sb ) { return sb.append("</p>\n"); }

    @Override StringBuilder listHead( StringBuilder sb ) { return sb.append("<ul>"); }
    @Override StringBuilder listBullet( StringBuilder sb, String s, String body, int d ) {
      return paragraph(sb.append("<li>").append(s).append("</li>"),body).append('\n');
    }
    @Override StringBuilder listTail( StringBuilder sb ) { return sb.append("</ul>\n"); }
    @Override String bold( String s ) { return "<b>"+s+"</b>"; }

    public StringBuilder arrayHead( StringBuilder sb ) { return arrayHead(sb,null); }
    public StringBuilder arrayHead( StringBuilder sb, String[] headers ) {
      sb.append("<span style='display: inline-block;'>");
      sb.append("<table class='table table-striped table-bordered'>\n");
      if( headers != null ) {
        sb.append("<tr>");
        for( String s : headers ) sb.append("<th>").append(s).append("</th>");
        sb.append("</tr>\n");
      }
      return sb;
    }
    public StringBuilder arrayTail( StringBuilder sb ) { return sb.append("</table></span>\n"); }
    public StringBuilder array( StringBuilder sb, String[] ss ) {
      arrayHead(sb);
      for( String s : ss ) sb.append("<tr><td>").append(s).append("</td></tr>");
      return arrayTail(sb);
    }
  }

  // --------------------------------------------------------------------------
  // ReST flavored help text
  static class ReST extends DocGen { // Restructured text
    private StringBuilder cr(StringBuilder sb) { return sb.append('\n'); }
    private StringBuilder underLine( StringBuilder sb, String s, char c ) {
      cr(cr(sb).append(s));
      int len = s.length();
      for( int i=0; i<len; i++ ) sb.append(c);
      return cr(cr(sb));
    }
    @Override StringBuilder escape(StringBuilder sb, String s ) { return sb.append(s); }
    @Override StringBuilder bodyHead( StringBuilder sb ) { return sb; }
    @Override StringBuilder bodyTail( StringBuilder sb ) { return sb; }
    @Override StringBuilder title  ( StringBuilder sb, String t ) { return underLine(sb,t,'='); }
    @Override StringBuilder section( StringBuilder sb, String t ) { return underLine(sb,t,'-'); }
    @Override StringBuilder listHead( StringBuilder sb ) { return cr(sb); }
    @Override StringBuilder listBullet( StringBuilder sb, String s, String body, int d ) {
      if( d > 0 ) sb.append("  ");
      cr(sb.append("*  ").append(s));
      if( body.length() > 0 )
        cr(cr(cr(sb).append("   ").append(body)));
      return sb;
    }
    @Override StringBuilder listTail( StringBuilder sb ) { return cr(sb); }
    @Override String bold( String s ) { return "**"+s+"**"; }
    @Override StringBuilder paraHead( StringBuilder sb ) { return sb.append("  "); }
    @Override StringBuilder paraTail( StringBuilder sb ) { return cr(sb); }
  }
}
