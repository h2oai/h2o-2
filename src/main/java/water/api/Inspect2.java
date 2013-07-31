package water.api;

import water.*;
import water.Weaver.Weave;
import water.fvec.*;

public class Inspect2 extends Request {
  static final int API_WEAVER=1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  // This Request supports the HTML 'GET' command, and this is the help text
  // for GET.
  static final String DOC_GET = "Inspect a fluid-vec frame";

  @Weave(help="An existing H2O Frame key.")
  final FrameKey src_key = new FrameKey("src_key");

  public static Response redirect(Request req, String src_key) {
    return new Response(Response.Status.redirect, req, -1, -1, "Inspect2", "src_key", src_key );
  }

  @Override protected Response serve() {
    Frame fr = DKV.get(src_key.value()).get();
    if( fr == null ) return RequestServer._http404.serve();
    return new Response(Response.Status.done, this, -1, -1, null);
  }

  @Override public boolean toHTML( StringBuilder sb ) {
    Key skey = src_key.value();
    Frame fr = DKV.get(skey).get();
    final int numCols = fr.numCols();

    DocGen.HTML.title(sb,skey.toString());
    DocGen.HTML.arrayHead(sb);
    // Column labels
    sb.append("<tr class='warning'>");
    sb.append("<td>").append("Row").append("</td>");
    for( int i=0; i<numCols; i++ ) 
      sb.append("<td><b>").append(fr._names[i]).append("</b></td>");
    sb.append("</tr>");

    //sb.append("<tr class='warning'>");
    //sb.append("<td>").append("Bytes").append("</td>");
    //for( int i=0; i<numCols; i++ ) 
    //  sb.append("<td>").append(PrettyPrint.bytes(fr._vecs[i].byteSize())).append("</td>");
    //sb.append("</tr>");

    sb.append("<tr class='warning'>");
    sb.append("<td>").append("Min").append("</td>");
    for( int i=0; i<numCols; i++ )
      sb.append("<td>").append(x3(fr._vecs[i].min())).append("</td>");
    sb.append("</tr>");

    sb.append("<tr class='warning'>");
    sb.append("<td>").append("Max").append("</td>");
    for( int i=0; i<numCols; i++ ) 
      sb.append("<td>").append(x3(fr._vecs[i].max())).append("</td>");
    sb.append("</tr>");

    boolean hasNAs = false;
    long nas[] = new long[numCols];
    for( int i=0; i<numCols; i++ ) 
      if( (nas[i]=fr._vecs[i].NAcnt()) > 0 ) hasNAs = true;

    if( hasNAs ) {
      sb.append("<tr class='warning'>");
      sb.append("<td>").append("Missing").append("</td>");
      for( int i=0; i<numCols; i++ ) 
        sb.append("<td>").append(nas[i] > 0 ? Long.toString(nas[i]) : "").append("</td>");
      sb.append("</tr>");
    }

    // First N rows
    int N = (int)Math.min(100,fr.numRows());
    for( int j=0; j<N; j++ ) {
      sb.append("<tr>");
      sb.append("<td>").append(j).append("</td>");
      for( int i=0; i<numCols; i++ ) 
        sb.append("<td>").append(x1(fr._vecs[i],j)).append("</td>");
      sb.append("</tr>");
    }

    DocGen.HTML.arrayTail(sb);

    //throw H2O.unimpl();
    return true;
  }

  // ---
  // Return a well-formated string for this kind of Vec
  private String x1( Vec v, int row ) {
    switch( v.dtype() ) {
    case I: 
      return Long.toString(v.at8(row));
    case F: {
      Chunk c = v.elem2BV(0);
      Class Cc = c.getClass();
      if(        Cc == null ) {
      } else if( Cc == C1SChunk.class ) {
        return x2(v,row,((C1SChunk)c)._scale);
      } else if( Cc == C2SChunk.class ) {
        return x2(v,row,((C2SChunk)c)._scale);
      } else {
        return Double.toString(v.at (row));
        //throw H2O.unimpl();
      }
      return Double.toString(v.at (row));
    }
    case S:
      return v.domain(v.at8(row));
    default: throw H2O.unimpl();
    }
  }

  private String x2( Vec v, int row, double scale ) {
    String s = Double.toString(v.at(row));
    // Double math roundoff error means sometimes we get very long trailing
    // strings of junk 0's with 1 digit at the end... when we *know* the data
    // has only "scale" digits.  Chop back to actual digits
    int ex = (int)Math.log10(scale);
    int x = s.indexOf('.');
    int y = x+1+(-ex);
    if( x != -1 && y < s.length() ) s = s.substring(0,x+1+(-ex));
    while( s.charAt(s.length()-1)=='0' )
      s = s.substring(0,s.length()-1);
    return s;
  }

  private String x3( double d ) {
    return (long)d==d ? Long.toString((long)d) : Double.toString(d);
  }

  // ---
  public class FrameKey extends TypeaheadInputText<Key> {
    public FrameKey(String name) { super(TypeaheadHexKeyRequest.class, name, true); }

    @Override protected Key parse(String input) throws IllegalArgumentException {
      Key k = Key.make(input);
      Value v = DKV.get(k);
      if (v == null)    throw new IllegalArgumentException("Key "+input+" not found!");
      Iced ice = v.get();
      if( !(ice instanceof Frame) ) throw new IllegalArgumentException("Key "+input+" is not a valid Frame key");
      return k;
    }
    @Override protected Key defaultValue() { return null; }
    @Override protected String queryDescription() { return "An existing H2O Frame key."; }
  }
}
