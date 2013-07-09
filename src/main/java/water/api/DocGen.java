package water.api;

import water.*;
import water.api.Request.*;

/** 
 * Auto-gen doc support, for JSON & REST API docs
 * @author <a href="mailto:cliffc@0xdata.com"></a>
 */
public abstract class DocGen {
  static final HTML HTML = new HTML();
  static final ReST ReST = new ReST();

  public static void main(String[] args) {
    H2O.main(args);
    TestUtil.stall_till_cloudsize(1);
    System.out.println(new ImportFiles().ReSTHelp());
    //System.exit(0);
  }

  // --------------------------------------------------------------------------
  abstract StringBuilder bodyHead( StringBuilder sb );
  abstract StringBuilder bodyTail( StringBuilder sb );
  abstract StringBuilder title( StringBuilder sb, String t );
  abstract StringBuilder section( StringBuilder sb, String t );
  abstract StringBuilder paragraph( StringBuilder sb, String s );
  abstract StringBuilder listHead( StringBuilder sb );
  abstract StringBuilder listBullet( StringBuilder sb, String s, String body );
  abstract StringBuilder listTail( StringBuilder sb );

  public String genHelp(Request R) {
    String name = R.getClass().getSimpleName();
    StringBuilder sb = new StringBuilder();
    bodyHead(sb);
    title(sb,name);
    paragraph(sb,"");
    section(sb,"Supported HTTP methods and descriptions");
    String gs = R.toGETDoc();
    if( gs != null ) {
      paragraph(sb,"GET");
      paragraph(sb,gs);
    }
    section(sb,"URL");
    paragraph(sb,"http://<h2oHost>:<h2oApiPort>/"+name+".json");
    section(sb,"Input parameters");
    section(sb,"Output JSON elements");
    JSONDoc docs[] = R.toJSONDoc();
    if( docs != null ) {
      listHead(sb);
      for( JSONDoc doc : docs )
        listBullet(sb,
                   doc._name+", a "+doc._clazz.getSimpleName(),
                   doc._help+".  From version "+doc._min_ver+
                   (doc._max_ver==Integer.MAX_VALUE?" onward":" to version "+doc._max_ver));
      listTail(sb);
    }
    bodyTail(sb);
    return sb.toString();
  }


  // --------------------------------------------------------------------------
  // HTML flavored help text
  static class HTML extends DocGen {
    private static StringBuilder escape(StringBuilder sb, String s ) {
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
    @Override StringBuilder title  ( StringBuilder sb, String t ) { return sb.append("<h3>").append(t).append("</h3>"); }
    @Override StringBuilder section( StringBuilder sb, String t ) { return sb.append("<h4>").append(t).append("</h4>"); }
    @Override StringBuilder paragraph( StringBuilder sb, String s ) { 
      return escape(sb.append("<blockquote>"),s).append("</blockquote>");
    }

    @Override StringBuilder listHead( StringBuilder sb ) { return sb.append("<ul>"); }
    @Override StringBuilder listBullet( StringBuilder sb, String s, String body ) {
      return sb.append("<li><b>").append(s).append("</b></li>").append(body);
    }
    @Override StringBuilder listTail( StringBuilder sb ) { return sb.append("</ul>"); }

    public StringBuilder arrayHead( StringBuilder sb ) { return arrayHead(sb,null); }
    public StringBuilder arrayHead( StringBuilder sb, String[] headers ) {
      sb.append("<span style='display: inline-block;'>");
      sb.append("<table class='table table-striped table-bordered'>");
      if( headers != null ) {
        sb.append("<tr>");
        for( String s : headers ) sb.append("<th>").append(s).append("</th>");
        sb.append("</tr>");
      }
      return sb;
    }
    public StringBuilder arrayTail( StringBuilder sb ) { return sb.append("</table></span>"); }
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
    @Override StringBuilder bodyHead( StringBuilder sb ) { return sb; }
    @Override StringBuilder bodyTail( StringBuilder sb ) { return sb; }
    @Override StringBuilder title  ( StringBuilder sb, String t ) { return underLine(sb,t,'='); }
    @Override StringBuilder section( StringBuilder sb, String t ) { return underLine(sb,t,'-'); }
    @Override StringBuilder paragraph( StringBuilder sb, String s ) { return cr(sb.append("  ").append(s)); }
    @Override StringBuilder listHead( StringBuilder sb ) { return sb; }
    @Override StringBuilder listBullet( StringBuilder sb, String s, String body ) {
      cr(sb.append("  "  ).append(s   ));
      cr(sb.append("    ").append(body));
      return sb;
    }
    @Override StringBuilder listTail( StringBuilder sb ) { return sb; }
  }
}
