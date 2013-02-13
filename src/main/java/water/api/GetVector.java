
package water.api;

import water.ValueArray;
import water.exec.VAIterator;

import com.google.gson.*;

public class GetVector extends JSONOnlyRequest {
  public static int MAX_REQUEST_ITEMS = 200000;

  protected H2OHexKey _key = new H2OHexKey(KEY);
  protected Int _maxRows = new Int(Constants.MAX_ROWS,Integer.MAX_VALUE);

  @Override
  protected Response serve() {
    JsonObject result = new JsonObject();
    try {
      ValueArray va = _key.value();

      VAIterator iter = new VAIterator(va._key, 0, 0);

      long maxRows = Math.min(MAX_REQUEST_ITEMS / iter._ary.numCols(), iter._ary.numRows());



      maxRows = Math.min(_maxRows.value(),maxRows);  // we need this because R uses e+ format even for integers

      JsonArray columns = new JsonArray();
      JsonArray[] cols = new JsonArray[iter._ary.numCols()];
      for (int i = 0; i < cols.length; ++i)
        cols[i] = new JsonArray();
      for (int j = 0; j < maxRows; ++j) {
        iter.next();
        for (int i = 0 ; i < cols.length; ++i)
          cols[i].add( iter.isNA(i)
                       ? new JsonPrimitive("NaN")
                       : new JsonPrimitive(String.valueOf(iter.datad(i))));
      }
      for (int i = 0; i < cols.length; ++i) {
        JsonObject col = new JsonObject();
        String name = iter._ary._cols[i]._name;
        col.addProperty(Constants.NAME, (name == null || name.isEmpty()) ? String.valueOf(i) : name);
        col.add(Constants.CONTENTS,cols[i]);
        columns.add(col);
      }
      result.addProperty(KEY,va._key.toString());
      result.add(COLS, columns);
      result.addProperty(Constants.NUM_ROWS,iter._ary.numRows());
      result.addProperty(Constants.NUM_COLS,iter._ary.numCols());
      result.addProperty(Constants.SENT_ROWS,maxRows);
    } catch (Exception e) {
      return Response.error(e.toString());
    }
    return Response.done(result);
  }

}
