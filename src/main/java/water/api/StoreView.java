
package water.api;

import java.io.IOException;
import java.util.Arrays;

import water.*;
import water.parser.CsvParser;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

public class StoreView extends Request {

  protected Str _filter = new Str(FILTER, "");
  protected final Int _offset = new Int(OFFSET,0,0,1024);
  protected final Int _view = new Int(VIEW, 20, 0, 1024);

  @Override protected Response serve() {
    JsonObject result = new JsonObject();
    // get the offset index
    int offset = _offset.value();
    // write the response
    H2O cloud = H2O.CLOUD; // Current eldest Cloud
    Key[] keys = new Key[_view._max]; // Limit size of what we'll display on this page
    int len = 0;
    String filter = _filter.value();
    // Gather some keys that pass all filters
    for( Key key : H2O.keySet() ) {
      if( filter != null && // Have a filter?
          key.toString().indexOf(filter) == -1 )
        continue; // Ignore this filtered-out key
      if( !key.user_allowed() ) // Also filter out for user-keys
        continue;
      if( H2O.get(key) == null ) continue; // Ignore misses
      keys[len++] = key; // Capture the key
      if( len == keys.length ) {
        // List is full; stop
        result.addProperty(Constants.MORE,true);
        break;
      }
    }
    // sort the keys, for pretty display & reliable ordering
    Arrays.sort(keys,0,len);
    if ((offset>=len) && (offset != 0))
      return Response.error("Only "+len+" keys available");

    // Now build the result JSON with all available keys
    JsonArray ary = new JsonArray();
    for( int i=offset; i<offset+_view.value(); i++ ) {
      if( i >= len ) break;
      Value val = H2O.get(keys[i]);
      ary.add(formatKeyRow(cloud,keys[i],val));
    }

    result.add(KEYS,ary);
    result.addProperty(NUM_KEYS, len);
    result.addProperty(CLOUD_NAME, H2O.NAME);
    result.addProperty(NODE_NAME, H2O.SELF.toString());
    Response r = Response.done(result);
    r.addHeader(
        "<form class='well form-inline' action='StoreView.html'>" +
        " <input type='text' class='input-small span10' placeholder='filter' " +
        "    name='filter' id='filter' value='"+_filter.value()+"' maxlength='512'>" +
        " <button type='submit' class='btn btn-primary'>Filter keys!</button>" +
        "</form>");

    r.setBuilder(KEYS, new PaginatedTable(argumentsToJson(),offset,_view.value(), len, false));
    r.setBuilder(KEYS+"."+KEY, new KeyCellBuilder());
    r.setBuilder(KEYS+".col_0", new KeyMinAvgMaxBuilder());
    r.setBuilder(KEYS+".col_1", new KeyMinAvgMaxBuilder());
    r.setBuilder(KEYS+".col_2", new KeyMinAvgMaxBuilder());
    r.setBuilder(KEYS+".col_3", new KeyMinAvgMaxBuilder());
    r.setBuilder(KEYS+".col_4", new KeyMinAvgMaxBuilder());
    r.setBuilder(MORE, new HideBuilder());
    return r;
  }

  static private String noNaN( double d ) {
    return (Double.isNaN(d) || Double.isInfinite(d)) ? "" : Double.toString(d);
  }

  private JsonObject formatKeyRow(H2O cloud, Key key, Value val) {
    JsonObject result = new JsonObject();
    result.addProperty(KEY, key.toString());
    result.addProperty(VALUE_SIZE,val.length());

    JsonObject mt = new JsonObject();
    JsonObject jcols[] = new JsonObject[]{mt,mt,mt,mt,mt};
    long rows = -1;
    int cols = -1;
    String str = "";

    // See if this is a structured ValueArray. Report results from a total parse.
    if( val.isArray() ) {
      ValueArray ary = ValueArray.value(val);
      if( ary._cols.length > 1 || ary._cols[0]._size != 1 ) {
        rows = ary._numrows;
        cols = ary._cols.length;
        result.addProperty(ROWS,rows); // exact rows
        result.addProperty(COLS,cols); // exact cols
        for( int i = 0; i < jcols.length; ++i ) {
          JsonObject col = new JsonObject();
          if (i < cols) {
            ValueArray.Column c = ary._cols[i];
            if (c._size!=0) {
              col.addProperty(HEADER,c._name);
              if( c._domain==null ) {
                col.addProperty(MIN , noNaN(c._min ));
                col.addProperty(MEAN, noNaN(c._mean));
                col.addProperty(MAX , noNaN(c._max ));
              } else if( c._domain.length > 0 ) {
                int max = c._domain.length;
                col.addProperty(MIN , c._domain[0]);
                col.addProperty(MEAN, c._domain[max/2]);
                col.addProperty(MAX , c._domain[max-1]);
              }
            }
          }
          jcols[i] = col;
        }
      }
    }

    // If not a proper ValueArray, estimate by parsing the first 1meg chunk
    if( rows == -1 ) {
      CsvParser.Setup setup = Inspect.csvGuessValue(val);
      if( setup._data != null && setup._data[1].length > 0 ) { // Able to parse sanely?
        int zipped_len = val.getFirstBytes().length;
        double bytes_per_row = (double) zipped_len / setup._numlines;
        rows = (long) (val.length() / bytes_per_row);
        cols = setup._data[1].length;
        result.addProperty(ROWS, "~" + rows);
        result.addProperty(COLS, cols);
        for( int i=0; i<Math.min(cols,jcols.length); i++ ) {
          JsonObject col = new JsonObject();
          col.addProperty(HEADER,setup._data[0][i]); // First 4 rows, including labels
          col.addProperty(MIN   ,setup._data[1][i]); // as MIN/MEAN/MAX
          col.addProperty(MEAN  ,setup._data[2][i]);
          col.addProperty(MAX   ,setup._data[3][i]);
          jcols[i] = col;
        }
      } else {
        result.addProperty(ROWS,""); // no rows
        result.addProperty(COLS,"");
      }
      // Now the first 100 bytes of Value as a String
      StringBuilder sb = new StringBuilder();
      byte[] b = setup._bits;   // Unzipped bits, if any
      int newlines=0;
      int len = Math.min(b.length,100);
      for( int i=0; i<len; i++ ) {
        byte c = b[i];
        if( c == '&' ) sb.append("&amp;");
        else if( c == '<' ) sb.append("&lt;");
        else if( c == '>' ) sb.append("&gt;");
        else if( c == '\r' ) ;    // ignore windows crlf
        else if( c == '\n' ) {    // newline
          if( ++newlines >= 4 ) break; // limit to 4 lines visually
          sb.append("<br>");           // visual newline
        } else if( c == ',' && i+1<len && b[i+1]!=' ' )
          sb.append(", ");
        else if( c < 32 ) sb.append('?');
        else sb.append((char)c);
      }
      if( val.length() > len ) sb.append("...");
      str = sb.toString();
    }

    for( int i=0; i<jcols.length; i++ )
      result.add("col_"+i,jcols[i]);

    result.addProperty(VALUE,str); // VALUE last in the JSON
    return result;
  }
}
