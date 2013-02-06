package water.web;
import com.google.gson.JsonObject;
import hex.NOPTask;
import java.util.Properties;
import water.*;

public class NOP extends H2OPage {

  @Override
  public JsonObject serverJson(Server s, Properties p, String sessionID) throws PageError {
    JsonObject res = new JsonObject();
    Key k = ServletUtil.check_key(p, "Key");
    Value v = DKV.get(k);
    if(v == null){
      res.addProperty("error", "no such key in K/V store!");
      return res;
    }
    if( v._isArray==0 ){
      byte [] mem = v.get();
      int r = 0;
      for(byte b:mem) r ^= b;
      res.addProperty("result", r);
      return res;
    }
    NOPTask tsk = new NOPTask();
    tsk.invoke(k);
    res.addProperty("result", tsk.toString());
    return res;
  }
  @Override
  protected String serveImpl(Server server, Properties args, String sessionID) throws PageError {
    JsonObject json = serverJson(server, args, sessionID);
    if( json.has("error") )
      return H2OPage.error(json.get("error").getAsString());
    return json.toString();
  }
}

