package water.web;

import com.google.gson.*;
import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Properties;
import water.*;

public class Inspect extends H2OPage {

  static DecimalFormat dformat = new DecimalFormat("###.####");

  static String format(double d){
    if(Double.isNaN(d))return "";
    return dformat.format(d);
  }
  public Inspect() {
    // No thanks on the refresh, it's hard to use.
    //_refresh = 5;
  }

  @Override public String[] requiredArguments() {
    return new String[] { "Key" };
  }

  @Override public JsonObject serverJson(Server server, Properties parms, String sessionID) throws PageError {

    Key key = ServletUtil.check_key(parms,"Key");
    Value val = DKV.get(key);
    if( val == null )
      throw new PageError("Key not found: " + key.toString());

    JsonObject result = new JsonObject();
    result.addProperty("key", key.toString());
    if( val._isArray != 0 ) {
      result.addProperty("type", "ary");
      ValueArray ary = ValueArray.value(val);
      result.addProperty("rows", ary._numrows);
      result.addProperty("cols", ary._cols.length);
      result.addProperty("rowsize",ary._rowsize);
      result.addProperty("size",ary.length());
      JsonArray columns = new JsonArray();
      for( ValueArray.Column C : ary._cols ) {
        JsonObject col = new JsonObject();
        col.addProperty("name", C._name);
        col.addProperty("off", (int)C._off);
        if( C._domain != null ) {
          col.addProperty("type", "enum");
          JsonArray enums = new JsonArray();
          for( String e : C._domain )
            enums.add(new JsonPrimitive(e));
          col.add("enumdomain", enums);
        } else {
          col.addProperty("type", C._size > 0 ? "int" : "float");
        }
        col.addProperty("size", Math.abs(C._size));
        col.addProperty("base", C._base);
        col.addProperty("scale", C._scale);
        col.addProperty("min", String.valueOf(C._min));
        col.addProperty("max", String.valueOf(C._max));
        col.addProperty("badat", String.valueOf(ary._numrows-C._n));
        col.addProperty("mean", String.valueOf(C._mean));
        col.addProperty("var", String.valueOf(C._sigma));
        columns.add(col);
      }
      result.add("columns", columns);
    } else {
      result.addProperty("type", "value");
    }
    return result;
  }

  @Override protected String serveImpl(Server server, Properties args, String sessionID) throws PageError {
    Key key = ServletUtil.check_key(args,"Key");
    String ks = key.toString();

    // Distributed get
    Value val = DKV.get(key);
    if( val == null )
      return wrap(error("Key not found: "+ ks));

    if( val.isHex() )
      return structured_array(key,ValueArray.value(val), val.onHDFS());

    RString response = new RString(html);

    formatKeyRow(key,val,response);

    response.replace("key",key);

    if( H2O.OPT_ARGS.hdfs != null && !val.onHDFS() ) {
      RString hdfs = new RString("<a href='Store2HDFS?Key=%$key'><button class='btn btn-primary btn-mini'>store on HDFS</button></a>");
      hdfs.replace("key", key);
      response.replace("storeHdfs", hdfs.toString());
    } else {
      response.replace("storeHdfs", "");
    }

    // ASCII file? Give option to do a binary parse
    String p_keys = ks;
    int idx = ks.lastIndexOf('.');
    if( idx != -1 )
      p_keys = ks.substring(0,idx);
    p_keys += ".hex";
    if(p_keys.startsWith("hdfs://"))
      p_keys = p_keys.substring(7);
    else if (p_keys.startsWith("nfs:"+File.separator))
      p_keys = p_keys.substring(5);
    if( p_keys.equals(ks) ) p_keys += "2";

    Key p_key = Key.make(p_keys);
    boolean missed = DKV.get(p_key) == null;

    RString r = new RString(html_parse);
    r.replace("key", missed ? key : p_key);
    r.replace("parseKey", p_key);
    r.replace("pfunc", missed ? "Parse" : "Inspect");
    response.replace("parse", r.toString());

    return response.toString();
  }

  private void formatKeyRow(Key key, Value val, RString response) {
    RString row = response.restartGroup("tableRow");

    // Now the first 100 bytes of Value as a String
    byte[] b = new byte[100]; // Amount to read
    int len=0;
    try {
      len = val.openStream().read(b); // Read, which might force loading.
    } catch( IOException e ) {}
    StringBuilder sb = new StringBuilder();
    for( int i=0; i<len; i++ ) {
      byte c = b[i];
      if( c == '&' ) sb.append("&amp;");
      else if( c == '<' ) sb.append("&lt;");
      else if( c == '>' ) sb.append("&gt;");
      else if( c == '\n' ) sb.append("<br>");
      else if( c == ',' && i+1<len && b[i+1]!=' ' )
        sb.append(", ");
      else sb.append((char)c);
    }
    final long length = val.length();
    if( length > len ) sb.append("...");
    row.replace("value",sb);
    row.replace("size", PrettyPrint.bytes(length));

    ServletUtil.createBestEffortSummary(key, row, length);
  }


  final static String html =
      "<h1><a style='%delBtnStyle' href='RemoveAck?Key=%$key'><button class='btn btn-danger btn-mini'>X</button></a>&nbsp;&nbsp;<a href='/Get?Key=%$key'>%key</a>%execbtn</h1>"
    + "%storeHdfs"
    + "<table class='table table-striped table-bordered table-condensed'>"
    + "<colgroup><col/><col/><col/><col/><col colspan=5 align=center/></colgroup>\n"
    + "<thead><tr><th> <th> <th> <th align=center colspan=5>Min / Average / Max <th> </tr>\n"
    + " <tr><th>Size<th>Rows<th>Cols<th>Col 0<th>Col 1<th>Col 2<th>Col 3<th>Col 4<th>Value</tr></thead>\n"
    + "<tbody>\n"
    + "%tableRow{\n"
    + " <tr>"
    + " <td>%size</td>"
    + " <td>&#126;%rows</td>"
    + " <td>%cols</td>"
    + " <td>%col0</td>"
    + " <td>%col1</td>"
    + " <td>%col2</td>"
    + " <td>%col3</td>"
    + " <td>%col4</td>"
    + " <td>%value</td>"
    + " </tr>\n"
    + "}\n"
    + "</tbody>\n"
    + "</table>\n"
    + "%parse";

  final static String html_parse =
    "<a href='/%pfunc?Key=%$key&Key2=%$parseKey'>Basic Text-File Parse into %parseKey</a>\n";

  // ---------------------
  // Structured Array / Dataset display

  String structured_array( Key key, ValueArray ary, boolean onHDFS ) {
    RString response = new RString(html_ary);
    if( H2O.OPT_ARGS.hdfs != null && onHDFS ) {
      RString hdfs = new RString("<a href='Store2HDFS?Key=%$key'><button class='btn btn-primary btn-mini'>store on HDFS</button></a>");
      hdfs.replace("key", key);
      response.replace("storeHdfs", hdfs.toString());
    } else {
      response.replace("storeHdfs", "");
    }
    // Pretty-print the key
    response.replace("key",key);
    response.replace("rows",ary._numrows);
    response.replace("rowsize", ary._rowsize);
    response.replace("size", PrettyPrint.bytes(ary.length()));
    response.replace("ncolumns",ary._cols.length);

    // Header row
    StringBuilder sb = new StringBuilder();
    final int num_col = Math.min(255,ary._cols.length);
    for( int i=0; i<num_col; i++ )
      sb.append("<th>").append(ary._cols[i]._name);
    response.replace("head_row",sb);

    // Data layout scheme
    sb = new StringBuilder();
    for( int i=0; i<num_col; i++ )
      sb.append("<td> +").append((int)ary._cols[i]._off).append("</td>");
    response.replace("offset_row",sb);

    sb = new StringBuilder();
    for( int i=0; i<num_col; i++ )
      sb.append("<td>").append(Math.abs(ary._cols[i]._size)).append("b</td>");
    response.replace("size_row",sb);
    sb = new StringBuilder();
    for( int i=0; i<num_col; i++ )
      sb.append("<td>").append(format(ary._cols[i]._mean)).append("</td>");
    response.replace("mean_row",sb);
    sb = new StringBuilder();
    for( int i=0; i<num_col; i++ )
      sb.append("<td>").append(format(ary._cols[i]._sigma)).append("</td>");
    response.replace("sigma_row",sb);
    // Compression/math function: Ax+B
    sb = new StringBuilder();
    for( int i=0; i<num_col; i++ ) {
      sb.append("<td>");
      int sz = ary._cols[i]._size;
      if( sz != 0 ) {
        sb.append("(X");
        int base = ary._cols[i]._base;
        if( base != 0 ) {
          if( base > 0 ) sb.append('+');
          sb.append(base);
        }
        sb.append(")");
        if( sz == 1 || sz == 2 ) {
          int s = ary._cols[i]._scale;
          if( s != 1.0 ) sb.append("/").append(s);
        }
      }
      sb.append("</td>");
    }
    response.replace("math_row",sb);

    // Min & max
    sb = new StringBuilder();
    for( int i=0; i<num_col; i++ ) {
      sb.append("<td>");
      int sz = ary._cols[i]._size;
      if( sz != 0 ) {
        double min = ary._cols[i]._min;
        if( sz > 0 && ary._cols[i]._scale == 1 ) sb.append((long)min); else sb.append(min);
        sb.append(" - ");
        double max = ary._cols[i]._max;
        if( sz > 0 && ary._cols[i]._scale == 1 ) sb.append((long)max); else sb.append(max);
      }
      sb.append("</td>");
    }
    response.replace("min_max_row",sb);

    // Missing data
    boolean found=false;
    for( int i=0; i<num_col; i++ )
      if( ary._cols[i]._n != ary._numrows ) {
        found=true;
        break;
      }
    if( found ) {
      RString row = response.restartGroup("tableRow");
      sb = new StringBuilder();
      sb.append("<td>Rows missing data</td>");
      for( int i=0; i<num_col; i++ ) {
        sb.append("<td>");
        long sz = ary._numrows - ary._cols[i]._n;
        sb.append(sz != 0 ? sz : "");
        sb.append("</td>");
      }
      row.replace("data_row",sb);
      row.append();
    }

    // If we have more than 7 rows, display the first & last 3 rows, else
    // display all the rows.
    long num_rows = ary._numrows;
    if( num_rows > 7 ) {
      display_row(ary,0,response,num_col);
      display_row(ary,1,response,num_col);
      display_row(ary,2,response,num_col);
      display_row(ary,-1,response,num_col); // Placeholder view
      display_row(ary,num_rows-3,response,num_col);
      display_row(ary,num_rows-2,response,num_col);
      display_row(ary,num_rows-1,response,num_col);
    } else {
      for( int i=0; i<num_rows; i++ )
        display_row(ary,i,response,num_col);
    }

    return response.toString();
  }

  static private void display_row(ValueArray ary, long r, RString response, int num_col) {
    RString row = response.restartGroup("tableRow");
    StringBuilder sb = new StringBuilder();
    sb.append("<td>Row ").append(r==-1 ? "..." : r).append("</td>");
    for( int i=0; i<num_col; i++ ) {
      if( r == -1 || !ary.isNA(r,i) ) {
        sb.append("<td>");
        int sz = ary._cols[i]._size;
        if( sz != 0 ) {
          if( r == -1 ) sb.append("...");
          else {
            if( ary._cols[i]._domain != null )
              sb.append(ary._cols[i]._domain[(int)ary.data(r,i)]);
            else if( ary._cols[i]._size >= 0 && ary._cols[i]._scale==1 )
              sb.append(ary.data(r,i)); // int/long
            else sb.append(ary.datad(r,i)); // float/double
          }
        }
        sb.append("</td>");
      } else {
        sb.append("<td style='background-color:IndianRed'>NA</td>");
      }
    }
    row.replace("data_row",sb);
    row.append();
  }

  final static String html_ary =
      "<h1><a style='%delBtnStyle' href='RemoveAck?Key=%$key'><button class='btn btn-danger btn-mini'>X</button></a>&nbsp;&nbsp;<a href='/Get?Key=%$key'>%key</a>%execbtn</h1>"
    + "%storeHdfs"
    + "<p>Generated from <a href=/Inspect?Key=%$priorKey>%priorKey</a> by '%xform'<p>"
    + "<b><font size=+1>%rowsize bytes-per-row * %rows Rows = Totalsize %size</font></b></em><br>"
    + "Parsed %ncolumns columns<br>"
    + "<table class='table table-striped table-bordered table-condensed'>"
    + "<thead><tr><th>Column %head_row</tr></thead>\n"
    + "<tbody>\n"
    + "  <tr><td>Record offset</td>%offset_row</tr>\n"
    + "  <tr><td>Column bytes</td>%size_row</tr>\n"
    + "  <tr><td>Internal scaling</td>%math_row</tr>\n"
    + "  <tr><td>Min/Max</td>%min_max_row</tr>\n"
    + "  <tr><td>&mu;</td>%mean_row</tr>\n"
    + "  <tr><td>&sigma;</td>%sigma_row</tr>\n"
    + "%tableRow{\n"
    + "  <tr>%data_row</tr>\n"
    + "}\n"
    + "</tbody>\n"
    + "</table>\n";
}
