package water.api;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import water.*;

public class LogView extends Request {
  @Override protected Response serve() {
    water.Log.LogStr logstr = UKV.get(water.Log.LOG_KEY);
    JsonArray ary = new JsonArray();
    if( logstr != null ) {
      for( int i=0; i<water.Log.LogStr.MAX; i++ ) {
        int x = (i+logstr._idx+1)&(water.Log.LogStr.MAX-1);
        if( logstr._dates[x] == null ) continue;
        JsonObject obj = new JsonObject();
        obj.addProperty("date", logstr._dates[x]);
        obj.addProperty("h2o" , logstr._h2os [x].toString());
        obj.addProperty("pid" , logstr._pids [x]);
        obj.addProperty("thr" , logstr._thrs [x]);
        obj.addProperty("msg" , logstr._msgs [x]);
        ary.add(obj);
      }
    }
    JsonObject result = new JsonObject();
    result.add("log",ary);
    return Response.done(result);
  }
}
