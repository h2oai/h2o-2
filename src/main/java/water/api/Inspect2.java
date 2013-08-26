package water.api;

import water.*;
import water.fvec.*;
import hex.gbm.DRF;

public class Inspect2 extends Request {
  static final int API_WEAVER=1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  // This Request supports the HTML 'GET' command, and this is the help text
  // for GET.
  static final String DOC_GET = "Inspect a fluid-vec frame";

  @API(help="An existing H2O Frame key.")
  final FrameKey src_key = new FrameKey("src_key");

  @API(help="Offset to begin viewing rows, or -1 to see a structural representation of the data")
  private final LongInt offset = new LongInt("offset", 0L, -1, Long.MAX_VALUE, "");

  @API(help="Number of data rows.") long numRows;
  @API(help="Number of data columns.") int numCols;
  @API(help="byte size in memory.") long byteSize;

  // An internal JSON-output-only class
  @SuppressWarnings("unused")
  private static class ColSummary extends Iced {
    static final int API_WEAVER=1; // This file has auto-gen'd doc & json fields
    static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.
    public ColSummary( String name, Vec vec ) {
      this.name = name;
      this.min  = vec.min();
      this.max  = vec.max();
      this.mean = vec.mean();
      this.NAcnt= vec.NAcnt();
      this.type = vec.dtype();
    }
    @API(help="Label."           ) final String name;
    @API(help="min."             ) final double min;
    @API(help="max."             ) final double max;
    @API(help="mean."            ) final double mean;
    @API(help="Missing elements.") final long   NAcnt;
    @API(help="Data type, one of I=Integer, F=Float, S=String, NA=all rows missing.")
    final Vec.DType type;
  }

  @API(help="Array of Column Summaries.")
  ColSummary cols[];


  // Called from some other page, to redirect that other page to this page.
  public static Response redirect(Request req, String src_key) {
    return new Response(Response.Status.redirect, req, -1, -1, "Inspect2", "src_key", src_key );
  }

  // Just validate the frame, and fill in the summary bits
  @Override protected Response serve() {
    Frame fr = DKV.get(src_key.value()).get();
    if( fr == null ) return RequestServer._http404.serve();
    numRows = fr.numRows();
    numCols = fr.numCols();
    byteSize = fr.byteSize();
    cols = new ColSummary[numCols];
    for( int i=0; i<cols.length; i++ )
      cols[i] = new ColSummary(fr._names[i],fr._vecs[i]);

    return new Response(Response.Status.done, this, -1, -1, null);
  }

  @Override public boolean toHTML( StringBuilder sb ) {
    final Key skey = src_key.value();
    final Frame fr = DKV.get(skey).get();
    final long off = offset.value();

    // Missing/NA count
    long NAcnt = 0;
    for( int i=0; i<cols.length; i++ )
      NAcnt += cols[i].NAcnt;

    DocGen.HTML.title(sb,skey.toString());
    DocGen.HTML.section(sb,""+numCols+" columns, "+numRows+" rows, "+
                        PrettyPrint.bytes(byteSize)+" bytes, "+
                        (NAcnt== 0 ? "no":PrettyPrint.bytes(NAcnt))+" missing elements");

    sb.append("<div class='alert'>" +
              //"View " + SummaryPage2.link(key, "Summary") +
              "<br/>Build models using " +
              DRF.link(src_key.value(), "Distributed Random Forest") +", "+
              hex.LR2.link(src_key.value(), "Linear Regression") +
              "</div>");

    // Start of where the pagination table goes.  For now, just the info button.
    sb.append("<div style='text-align:center;'>");
    sb.append("<span class='pagination'><ul><li>"+"<a href='"+
              RequestStatics.encodeRedirectArgs(null,new String[]{"src_key",skey.toString(),"offset",off>=0?"-1":"0"})+
              "'>"+(off>=0?"info":"rows")+"</a>"+"</li></ul></span>&nbsp;&nbsp;");
    sb.append("</div>");

    DocGen.HTML.arrayHead(sb);
    // Column labels
    sb.append("<tr class='warning'>");
    sb.append("<td>").append("Row").append("</td>");
    for( int i=0; i<cols.length; i++ )
      sb.append("<td><b>").append(cols[i].name).append("</b></td>");
    sb.append("</tr>");

    sb.append("<tr class='warning'>");
    sb.append("<td>").append("Min").append("</td>");
    for( int i=0; i<cols.length; i++ )
      sb.append("<td>").append(x1(fr._vecs[i],-1,cols[i].min)).append("</td>");
    sb.append("</tr>");

    sb.append("<tr class='warning'>");
    sb.append("<td>").append("Max").append("</td>");
    for( int i=0; i<cols.length; i++ )
      sb.append("<td>").append(x1(fr._vecs[i],-1,cols[i].max)).append("</td>");
    sb.append("</tr>");

    sb.append("<tr class='warning'>");
    sb.append("<td>").append("Mean").append("</td>");
    for( int i=0; i<cols.length; i++ )
      sb.append("<td>").append(String.format("%5.3f",cols[i].mean)).append("</td>");
    sb.append("</tr>");

    sb.append("<tr class='warning'>");
    sb.append("<td>").append("Type").append("</td>");
    for( int i=0; i<cols.length; i++ )
      sb.append("<td>").append(cols[i].type).append("</td>");
    sb.append("</tr>");

    // Missing / NA row is optional; skip it if the entire dataset is clean
    if( NAcnt > 0 ) {
      sb.append("<tr class='warning'>");
      sb.append("<td>").append("Missing").append("</td>");
      for( int i=0; i<cols.length; i++ )
        sb.append("<td>").append(cols[i].NAcnt > 0 ? Long.toString(cols[i].NAcnt) : "").append("</td>");
      sb.append("</tr>");
    }

    if( off == -1 ) {           // Info display
      sb.append("<tr class='warning'>");
      // An extra row holding vec's compressed bytesize
      sb.append("<td>").append("Size").append("</td>");
      for( int i=0; i<cols.length; i++ )
        sb.append("<td>").append(PrettyPrint.bytes(fr._vecs[i].byteSize())).append("</td>");
      sb.append("</tr>");

      // All Vecs within a frame are compatible, so just read the
      // home-node/data-placement and start-row from 1st Vec
      Vec c0 = fr.firstReadable();
      int N = c0.nChunks();
      for( int j=0; j<N; j++ ) { // All the chunks
        sb.append("<tr>");       // Row header
        // 1st column: report data home node (data placement), and row start
        sb.append("<td>").append(c0.chunkKey(j).home_node())
          .append(", ").append(c0.chunk2StartElem(j)).append("</td>");
        for( int i=0; i<cols.length; i++ ) {
          // Report chunk-type (compression scheme)
          String clazz = fr._vecs[i].elem2BV(j).getClass().getSimpleName();
          String trim = clazz.replaceAll("Chunk","");
          sb.append("<td>").append(trim).append("</td>");
        }
        sb.append("</tr>");
      }

    } else {                    // Row/data display
      // First N rows
      int N = (int)Math.min(100,numRows);
      for( int j=0; j<N; j++ ) {// N rows
        sb.append("<tr>");      // Row header
        sb.append("<td>").append(j).append("</td>");
        for( int i=0; i<cols.length; i++ ) // Columns w/in row
          sb.append("<td>").append(x0(fr._vecs[i],j)).append("</td>");
        sb.append("</tr>");
      }
    }

    DocGen.HTML.arrayTail(sb);

    return true;
  }

  // ---
  // Return a well-formated string for this kind of Vec
  private String x0( Vec v, int row ) { return x1(v,row,v.at(row)); }

  // Format a row, OR the min/max
  private String x1( Vec v, int row, double d ) {
    if( (row >= 0 && v.isNA(row)) || Double.isNaN(d) )
      return "-";               // Display of missing elements
    switch( v.dtype() ) {
    case I:
      return Long.toString(row >= 0 ? v.at8(row) : (long)d);
    case F: {
      Chunk c = v.elem2BV(0);
      Class Cc = c.getClass();
      if( Cc == C1SChunk.class ) return x2(d,((C1SChunk)c)._scale);
      if( Cc == C2SChunk.class ) return x2(d,((C2SChunk)c)._scale);
      return Double.toString(d);
    }
    case S:
      return row >= 0 ? v.domain(v.at8(row)) : Long.toString((long)d);
    default: throw H2O.unimpl();
    }
  }

  private String x2( double d, double scale ) {
    String s = Double.toString(d);
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
}
