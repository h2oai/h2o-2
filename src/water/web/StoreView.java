package water.web;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

import water.*;

import com.google.gson.*;
//import water.hdfs.PersistHdfs;

public class StoreView extends H2OPage {

  public static final int KEYS_PER_PAGE = 25;

  public StoreView() {
    // No thanks on the refresh, it's hard to use.
    //_refresh = 5;
  }

  @Override
  public JsonObject serverJson(Server server, Properties args, String sessionID) {
    JsonObject res = new JsonObject();
    final H2O cloud = H2O.CLOUD;

    // get the offset index
    int offset = 0;
    try {
      offset = Integer.valueOf(args.getProperty("o", "0"));
    } catch( NumberFormatException e ) { /* pass */ }

    Key[] keys = new Key[1024]; // Limit size of what we'll display on this page
    int len = 0;
    String filter = args.getProperty("Filter");
    String html_filter = (filter==null? "" : "?Filter="+filter);
    //PersistHdfs.refreshHDFSKeys();
    // Gather some keys that pass all filters
    for( Key key : H2O.keySet() ) {
      if( filter != null && // Have a filter?
          key.toString().indexOf(filter) == -1 )
        continue; // Ignore this filtered-out key
      if( !key.user_allowed() ) // Also filter out for user-keys
        continue;
      if( H2O.get(key) == null ) continue; // Ignore misses
      keys[len++] = key; // Capture the key
      if( len == keys.length ) break; // List is full; stop
    }

    // sort the keys, for pretty display & reliable ordering
    Arrays.sort(keys,0,len);
    JsonArray jary = new JsonArray();
    for( int i=0; i<len; i++ )
      jary.add(new JsonPrimitive(keys[i].toString()));
    res.add("keys",jary);
    return res;
  }

  @Override protected String serveImpl(Server server, Properties args, String sessionID) {
    RString response = new RString(html);
    // get the offset index
    int offset = 0;
    try {
      offset = Integer.valueOf(args.getProperty("o", "0"));
    } catch( NumberFormatException e ) { /* pass */ }
    // write the response
    H2O cloud = H2O.CLOUD; // Current eldest Cloud
    Key[] keys = new Key[1024]; // Limit size of what we'll display on this page
    int len = 0;
    String filter = args.getProperty("Filter");
    String html_filter = (filter==null? "" : "?Filter="+filter);
    //PersistHdfs.refreshHDFSKeys();
    // Gather some keys that pass all filters
    for( Key key : H2O.keySet() ) {
      if( filter != null && // Have a filter?
          key.toString().indexOf(filter) == -1 )
        continue; // Ignore this filtered-out key
      if( !key.user_allowed() ) // Also filter out for user-keys
        continue;
      if( H2O.get(key) == null ) continue; // Ignore misses
      keys[len++] = key; // Capture the key
      if( len == keys.length ) break; // List is full; stop
    }

    // sort the keys, for pretty display & reliable ordering
    Arrays.sort(keys,0,len);
    // Pagination, if the list is long
    formatPagination(offset,len, html_filter, response);
    offset *= KEYS_PER_PAGE;

    for( int i=offset; i<offset+KEYS_PER_PAGE; i++ ) {
      if( i >= len ) break;
      Value val = H2O.get(keys[i]);
      formatKeyRow(cloud,keys[i],val,response);
    }

    response.replace("noOfKeys",len);
    response.replace("cloud_name",H2O.NAME);
    response.replace("node_name",H2O.SELF.toString());
    if( filter!=null )
      response.replace("pvalue","value='"+filter+"'");
    return response.toString();
  }

  private void formatPagination(int offset, int size, String prefix, RString response) {
    if (size<=KEYS_PER_PAGE)
      return;
    StringBuilder sb = new StringBuilder();
    sb.append("<div class='pagination pagination-centered' style='margin:0px auto'><ul>");
    if (offset!=0) {
      sb.append("<li><a href='StoreView?o=0"+prefix+"'>First</li>");
      sb.append("<li><a href='StoreView?o="+(offset-1)+prefix+"'>&lt;&lt;</a></li>");
    }
    int j = 0;
    int i = offset - 5;
    while (j<10) {
      if (++i<0)
        continue;
      if (i>size/KEYS_PER_PAGE)
        break;
      if (i==offset)
        sb.append("<li class='active'><a href='StoreView'>"+i+"</li>");
      else
        sb.append("<li><a href='StoreView?o="+i+prefix+"'>"+i+"</li>");
      ++j;
    }
    if (offset < (size/KEYS_PER_PAGE)) {
      sb.append("<li><a href='StoreView?o="+(offset+1)+prefix+"'>&gt;&gt;</a></li>");
      sb.append("<li><a href='StoreView?o="+(size/KEYS_PER_PAGE)+prefix+"'>Last</a></li>");
    }
    sb.append("</ul></div>");
    String nav = sb.toString();
    response.replace("navup",nav);
  }

  private void formatKeyRow(H2O cloud, Key key, Value val, RString response) {
    RString row = response.restartGroup("tableRow");
    row.replace("key",key);
    if(new String(key._kb).startsWith("ConfusionMatrix"))
      row.replace("inspect","RFView");
    else
      row.replace("inspect", "Inspect");
    // Now the first 100 bytes of Value as a String
    byte[] b = new byte[100]; // Amount to read
    int len=0;
    try {
      len = val.openStream().read(b); // Read, which might force loading.
    } catch( IOException e ) {}
    StringBuilder sb = new StringBuilder();
    int newlines=0;
    for( int i=0; i<len; i++ ) {
      byte c = b[i];
      if( c == '&' ) sb.append("&amp;");
      else if( c == '<' ) sb.append("&lt;");
      else if( c == '>' ) sb.append("&gt;");
      else if( c == '\n' ) { sb.append("<br>"); if( newlines++ > 5 ) break; }
      else if( c == ',' && i+1<len && b[i+1]!=' ' )
        sb.append(", ");
      else sb.append((char)c);
    }
    if( val.length() > len ) sb.append("...");
    row.replace("value",sb);
    row.replace("size",val.length());

    if( H2O.OPT_ARGS.hdfs != null && !val.onHDFS() ) {
      RString hdfs = new RString("<a href='Store2HDFS?Key=%$key'><button class='btn btn-primary btn-mini'>store on HDFS</button></a>");
      hdfs.replace("key", key);
      row.replace("storeHdfs", hdfs.toString());
    } else {
      row.replace("storeHdfs", "");
    }

    // See if this is a structured ValueArray. Report results from a total parse.
    if( val._isArray != 0 ) {
      ValueArray ary = ValueArray.value(val);
      if( ary._cols.length > 1 || ary._cols[0]._size != 1 ) {
        row.replace("rows",ary._numrows);
        int cols = ary._cols.length;
        row.replace("cols",cols);
        for( int i=0; i<Math.min(cols,5); i++ ) {
          sb = new StringBuilder();
          int sz = ary._cols[i]._size;
          if( sz != 0 ) {
            double min = ary._cols[i]._min;
            if( sz > 0 && ary._cols[i]._scale == 1 ) sb.append((long)min); else sb.append(min);
            sb.append(" / - / ");
            double max = ary._cols[i]._max;
            if( sz > 0 && ary._cols[i]._scale == 1 ) sb.append((long)max); else sb.append(max);
          }
          row.replace("col"+i,sb);
        }
        row.append();
        return;
      }
    }
    ServletUtil.createBestEffortSummary(key, row, val.length());
  }

  final static String html =
    "<div class='alert alert-success'>"
    + "You are connected to cloud <strong>%cloud_name</strong> and node <strong>%node_name</strong>."
    + "</div>"
    + "<form class='well form-inline' action='StoreView'>"
    + " <input type='text' class='input-small span10' placeholder='filter' name='Filter' id='Filter' %pvalue maxlength='512'>"
    + " <button type='submit' class='btn btn-primary'>Filter keys!</button>"
    + "</form>"
    + "<p>Displaying %noOfKeys keys"
    + "<p>%navup</p>"
    + "<table class='table table-striped table-bordered table-condensed'>"
    + "<colgroup><col/><col/><col/><col/><col colspan=5 align=center/></colgroup>\n"
    + "<thead><tr><th> <th> <th> <th> <th align=center colspan=5>Min / Average / Max <th> </tr>\n"
    + " <tr><th>Key<th>Size<th>Rows<th>Cols<th>Col 0<th>Col 1<th>Col 2<th>Col 3<th>Col 4<th>Value</tr></thead>\n"
    + "<tbody>\n"
    + "%tableRow{\n"
    + " <tr>"
    + " <td>"
    + " <a style='%delBtnStyle' href='RemoveAck?Key=%$key'><button class='btn btn-danger btn-mini'>X</button></a>"
    + " %storeHdfs"
    + " &nbsp;&nbsp;<a href='/%inspect?Key=%$key'>%key</a>%execbtn"
    + " </td>"
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
    ;
}
