
package water.api.v2;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

import water.Key;
import water.api.*;
import water.api.RequestServer.API_VERSION;

  public class UploadFile extends JSONOnlyRequest {
    H2OKey key = new H2OKey(FILENAME,true);

    @Override public String href() { return href(API_VERSION.V_v2); }
    @Override protected String href(API_VERSION v) {
      return v.prefix() + "post_file";
    }

    @Override protected Response serve() {
      DateFormat dateFormat = new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss");
      Date date = new Date();
      String dstTime = "_"+dateFormat.format(date);

      JsonObject response = new JsonObject();
      //response.addProperty("dst", key.value().toString());
      response.add("uri", new JsonPrimitive(key.value().toString()));
      response.add("size", new JsonPrimitive(0));
      //response.add("dst", new JsonPrimitive(Key.make().toString()));
      String k = key.value().toString();
      String fileName = k.toString().substring( k.toString().lastIndexOf('/')+1, k.toString().length() );
      String fileNameWithoutExtn = fileName.substring(0, fileName.lastIndexOf('.'));
      response.add("dst", new JsonPrimitive(fileNameWithoutExtn+dstTime+".hex"));
      return Response.custom(response);
    }
  }
