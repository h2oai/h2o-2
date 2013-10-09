package water.api;

import water.*;
import water.fvec.*;
import hex.gbm.DRF;
import hex.gbm.GBM;

public class Inspect2 extends Request2 {
  static final int API_WEAVER=1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  // This Request supports the HTML 'GET' command, and this is the help text
  // for GET.
  static final String DOC_GET = "Inspect a fluid-vec frame";

  @API(help="An existing H2O Frame key.", required=true, filter=Default.class)
  Frame src_key;

  @API(help="Offset to begin viewing rows, or -1 to see a structural representation of the data", filter=Default.class, lmin=-1, lmax=Long.MAX_VALUE)
  long offset;

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
      this.naCnt= vec.naCnt();
    }
    @API(help="Label."           ) final String name;
    @API(help="min."             ) final double min;
    @API(help="max."             ) final double max;
    @API(help="mean."            ) final double mean;
    @API(help="Missing elements.") final long   naCnt;
  }

  @API(help="Array of Column Summaries.")
  ColSummary cols[];


  // Called from some other page, to redirect that other page to this page.
  public static Response redirect(Request req, String src_key) {
    return new Response(Response.Status.redirect, req, -1, -1, "Inspect2", "src_key", src_key );
  }

  // Just validate the frame, and fill in the summary bits
  @Override protected Response serve() {
    if( src_key == null ) return RequestServer._http404.serve();
    numRows = src_key.numRows();
    numCols = src_key.numCols();
    Futures fs = new Futures();
    for( int i=0; i<numCols; i++ )
      src_key.vecs()[i].rollupStats(fs);
    fs.blockForPending();

    byteSize = src_key.byteSize();
    cols = new ColSummary[numCols];
    for( int i=0; i<cols.length; i++ )
      cols[i] = new ColSummary(src_key._names[i],src_key.vecs()[i]);

    return new Response(Response.Status.done, this, -1, -1, null);
  }

  @Override public boolean toHTML( StringBuilder sb ) {
    Key skey = Key.make(input("src_key"));

    // Missing/NA count
    long naCnt = 0;
    for( int i=0; i<cols.length; i++ )
      naCnt += cols[i].naCnt;

    DocGen.HTML.title(sb,skey.toString());
    DocGen.HTML.section(sb,""+numCols+" columns, "+numRows+" rows, "+
                        PrettyPrint.bytes(byteSize)+" bytes (compressed), "+
                        (naCnt== 0 ? "no":PrettyPrint.bytes(naCnt))+" missing elements");

    sb.append("<div class='alert'>" +
              //"View " + SummaryPage2.link(key, "Summary") +
              "<br/>Build models using " +
              DRF.link(skey, "Distributed Random Forest") +", "+
              GBM.link(skey, "Distributed GBM") +", "+
              hex.LR2.link(skey, "Linear Regression") + ",<br>"+
              SummaryPage2.link(skey,"Summary")+", "+
              DownloadDataset.link(skey, "Download as CSV") +
              "</div>");
    String _scrollto = String.valueOf(offset - 1);
      sb.append(
      " <script>$(document).ready(function(){ " +
      " $('html, body').animate({ scrollTop: $('#row_"+_scrollto+"').offset().top" +
      "}, 2000);" +
      "return false;" +
      "});</script>");
    sb.append(
        "<form class='well form-inline' action='Inspect2.html' id='inspect'>" +
        " <input type='hidden' name='src_key' value="+skey.toString()+">" +
        " <input type='text' class='input-small span5' placeholder='filter' " +
        "    name='offset' id='offset' value='"+offset+"' maxlength='512'>" +
        " <button type='submit' class='btn btn-primary'>Jump to row!</button>" +
        "</form>");

    // Start of where the pagination table goes.  For now, just the info button.
    sb.append(pagination(src_key.numRows(), skey));

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
      sb.append("<td>").append(x1(src_key.vecs()[i],-1,cols[i].min)).append("</td>");
    sb.append("</tr>");

    sb.append("<tr class='warning'>");
    sb.append("<td>").append("Max").append("</td>");
    for( int i=0; i<cols.length; i++ )
      sb.append("<td>").append(x1(src_key.vecs()[i],-1,cols[i].max)).append("</td>");
    sb.append("</tr>");

    sb.append("<tr class='warning'>");
    sb.append("<td>").append("Mean").append("</td>");
    for( int i=0; i<cols.length; i++ )
      sb.append("<td>").append(String.format("%5.3f",cols[i].mean)).append("</td>");
    sb.append("</tr>");

    // Missing / NA row is optional; skip it if the entire dataset is clean
    if( naCnt > 0 ) {
      sb.append("<tr class='warning'>");
      sb.append("<td>").append("Missing").append("</td>");
      for( int i=0; i<cols.length; i++ )
        sb.append("<td>").append(cols[i].naCnt > 0 ? Long.toString(cols[i].naCnt) : "").append("</td>");
      sb.append("</tr>");
    }

    if( offset == -1 ) {           // Info display
      sb.append("<tr class='warning'>");
      // An extra row holding vec's compressed bytesize
      sb.append("<td>").append("Size").append("</td>");
      for( int i=0; i<cols.length; i++ )
        sb.append("<td>").append(PrettyPrint.bytes(src_key.vecs()[i].byteSize())).append("</td>");
      sb.append("</tr>");

      // All Vecs within a frame are compatible, so just read the
      // home-node/data-placement and start-row from 1st Vec
      Vec c0 = src_key.anyVec();
      int N = c0.nChunks();
      for( int j=0; j<N; j++ ) { // All the chunks
        sb.append("<tr>");       // Row header
        // 1st column: report data home node (data placement), and row start
        sb.append("<td>").append(c0.chunkKey(j).home_node())
          .append(", ").append(c0.chunk2StartElem(j)).append("</td>");
        for( int i=0; i<cols.length; i++ ) {
          // Report chunk-type (compression scheme)
          String clazz = src_key.vecs()[i].elem2BV(j).getClass().getSimpleName();
          String trim = clazz.replaceAll("Chunk","");
          sb.append("<td>").append(trim).append("</td>");
        }
        sb.append("</tr>");
      }

    } else {                    // Row/data display
      // First N rows
      int N = (int)Math.min(100,numRows-offset);
      for( int j=0; j<N; j++ ) {// N rows
        sb.append("<tr id='row_"+String.valueOf(offset+j)+"'>");      // Row header
        sb.append("<td>").append(offset+j).append("</td>");
        for( int i=0; i<cols.length; i++ ) // Columns w/in row
          sb.append("<td>").append(x0(src_key.vecs()[i],offset+j)).append("</td>");
        sb.append("</tr>");
      }
    }

    DocGen.HTML.arrayTail(sb);

    return true;
  }

  // ---
  // Return a well-formated string for this kind of Vec
  public static String x0( Vec v, long row ) { return x1(v,row,v.at(row)); }

  // Format a row, OR the min/max
  public static String x1( Vec v, long row, double d ) {
    if( (row >= 0 && v.isNA(row)) || Double.isNaN(d) )
      return "-";               // Display of missing elements
    if( v.isEnum() ) return row >= 0 ? v.domain(v.at8(row)) : Long.toString((long)d);
    if( v.isInt() )  return Long.toString(row >= 0 ? v.at8(row) : (long)d);
    Chunk c = v.elem2BV(0);
    Class Cc = c.getClass();
    if( Cc == C1SChunk.class ) return x2(d,((C1SChunk)c)._scale);
    if( Cc == C2SChunk.class ) return x2(d,((C2SChunk)c)._scale);
    return Double.toString(d);
  }

  public static String x2( double d, double scale ) {
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

  public String link(String txt,Key k, long offset, long max){
    if(offset != this.offset && 0 <= offset && offset <= max)return "<a href='Inspect2.html?src_key=" + k.toString() + "&offset=" + offset + "'>" + txt + "</a>";
    return "<span>" + txt + "</span>";
  }

  private String infoLink(Key k){
    return "<a href='Inspect2.html?src_key=" + k.toString() + "&offset=-1'>info</a>";
  }


  private static int viewsz = 100;

  protected String pagination(long max, Key skey) {
    final long offset = this.offset;
    StringBuilder sb = new StringBuilder();
    sb.append("<div style='text-align:center;'>");
    sb.append("<span class='pagination'><ul>");
    sb.append("<li>" + infoLink(skey) + "</li>");
    long lastOffset = (max / viewsz) * viewsz;
    long lastIdx = (max / viewsz);
    long currentIdx = offset / viewsz;
    long startIdx = Math.max(currentIdx-5,0);
    long endIdx = Math.min(startIdx + 11, lastIdx);
    if (offset == -1)
      currentIdx = -1;
    sb.append("<li>" + link("|&lt;",skey,0,lastOffset) + "</li>");
    sb.append("<li>" + link("&lt;",skey,offset-viewsz,lastOffset) + "</li>");
    for (long i = startIdx; i <= endIdx; ++i)
      sb.append("<li>" + link(String.valueOf(i),skey,i*viewsz,lastOffset) + "</li>");
    sb.append("<li>" + link("&gt;",skey,offset+viewsz,lastOffset) + "</li>");
    sb.append("<li>" + link("&gt;|",skey,lastOffset,lastOffset) + "</li>");
    sb.append("</ul></span>");
    sb.append("</div>");
    return sb.toString();
  }

}
