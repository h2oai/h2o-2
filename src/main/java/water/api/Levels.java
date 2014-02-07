package water.api;

import com.google.gson.*;

import water.*;
import water.ValueArray.Column;

public class Levels extends Request {
  protected final H2OHexKey _key = new H2OHexKey(KEY);
  protected final HexColumnSelect _columns = new HexColumnSelect(X, _key, 2500);
  protected final Int             _max_column   = new Int(COLUMNS_DISPLAY, MAX_COLUMNS_TO_DISPLAY);

  static final int MAX_COLUMNS_TO_DISPLAY = 1000;

  public static String link(Key k, String s){
    return "<a href='Levels.html?key="+k+"'>" + s + "</a>";
  }

  @Override protected Response serve() {
    int[] colIds = _columns.value();
    ValueArray ary = _key.value();

    final boolean did_trim_columns;
    final int max_columns_to_display;
    if (_max_column.value() >= 0)
      max_columns_to_display = Math.min(_max_column.value(), colIds.length == 0 ? ary._cols.length : colIds.length);
    else
      max_columns_to_display = Math.min(MAX_COLUMNS_TO_DISPLAY, colIds.length == 0 ? ary._cols.length : colIds.length);

    if(colIds.length == 0){
      did_trim_columns = ary._cols.length > max_columns_to_display;
      colIds = new int[ Math.min(ary._cols.length, max_columns_to_display) ];
      for(int i = 0; i < colIds.length; ++i) colIds[i] = i;
    } else if (colIds.length > max_columns_to_display){
      int [] cols2 = new int[ max_columns_to_display ];
      for (int j=0; j < max_columns_to_display; j++) cols2[ j ] = colIds[ j ];
      colIds = cols2;
      did_trim_columns = true;
    } else {
      did_trim_columns = false;
    }

    JsonObject res = new JsonObject();
    JsonArray levels = new JsonArray();
    for(int i = 0; i < colIds.length; i++) {
      Column c = ary._cols[colIds[i]];
      if(c.isEnum()) {
        JsonArray level_names = new JsonArray();
        for(int j = 0; j < c._domain.length; j++)
          level_names.add(new JsonPrimitive(c._domain[j]));
        levels.add(level_names);
      } else
        levels.add(null);
    }
    res.add("levels", levels);
    res.add("trim_cols", new JsonPrimitive(did_trim_columns));
    return Response.done(res);
  }

}
