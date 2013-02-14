package water.util;

import java.util.*;
import java.util.Map.Entry;

import com.google.common.base.Objects;
import com.google.common.collect.Maps;
import com.google.gson.*;

public class JsonUtil {
  private static final Map<JsonPrimitive, JsonPrimitive> SPECIAL = Maps.newHashMap();
  static {
    SPECIAL.put(new JsonPrimitive(Double.NaN), new JsonPrimitive("NaN"));
    SPECIAL.put(new JsonPrimitive(Double.POSITIVE_INFINITY), new JsonPrimitive("Infinity"));
    SPECIAL.put(new JsonPrimitive(Double.NEGATIVE_INFINITY), new JsonPrimitive("-Infinity"));
  }

  public static JsonObject escape(JsonObject json) {
    JsonObject res = new JsonObject();
    for( Entry<String, JsonElement> e : json.entrySet() )
      res.add(e.getKey(), escape(e.getValue()));
    return res;
  }
  public static JsonArray escape(JsonArray json) {
    JsonArray res = new JsonArray();
    for( JsonElement v : json ) res.add(escape(v));
    return res;
  }

  public static JsonElement escape(JsonElement v) {
    if( v.isJsonObject() ) return escape(v.getAsJsonObject());
    if( v.isJsonArray() ) return escape(v.getAsJsonArray());
    return Objects.firstNonNull(SPECIAL.get(v), v);
  }


}
