
package water.web;

import java.util.Properties;

import water.Key;
import water.ValueArray;
import water.exec.VAIterator;

import com.google.gson.*;

/**
 *
 * @author peta
 */
public class GetVector extends JSONPage {

  public static int MAX_REQUEST_ITEMS = 200000;

  @Override public JsonObject serverJson(Server server, Properties parms, String sessionID) throws PageError {
    JsonObject result = new JsonObject();
    try {
      ValueArray va = ServletUtil.check_array(parms, "key");

      VAIterator iter = new VAIterator(Key.make(parms.getProperty("Key")), 0, 0);

      long maxRows = Math.min(MAX_REQUEST_ITEMS / iter._ary.numCols(), iter._ary.numRows());

      maxRows = Math.min((long) Double.parseDouble(parms.getProperty("maxRows",String.valueOf(maxRows))),maxRows);  // we need this because R uses e+ format even for integers

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
        col.addProperty("name", (name == null || name.isEmpty()) ? String.valueOf(i) : name);
        col.add("contents",cols[i]);
        columns.add(col);
      }
      result.addProperty("key",va._key.toString());
      result.add("columns",columns);
      result.addProperty("num_rows",iter._ary.numRows());
      result.addProperty("num_cols",iter._ary.numCols());
      result.addProperty("sent_rows",maxRows);
    } catch (Exception e) {
      result.addProperty("Error", e.toString());
    }
    return result;
  }

  @Override public String[] requiredArguments() {
    return new String[] { "Key" };
  }
}



