package water.web;
import com.google.gson.JsonObject;
import java.util.Properties;
import water.Key;
import water.UKV;

public class Remove extends H2OPage {
  @Override public JsonObject serverJson(Server server, Properties args, String sessionID) throws PageError {
    JsonObject result = new JsonObject();
    try {
      Key key = ServletUtil.check_key(args,"Key");
      if (UKV.get(key)==null)
        throw new Exception("Key "+key.toString()+" does not exist.");
      UKV.remove(key);
      result.addProperty("Key",key.toString());
    } catch (Exception e) {
      result.addProperty("Error",e.getMessage());
    }
    return result;
  }
  
  @Override public String serveImpl(Server server, Properties args, String sessionID) throws PageError {
    Key key = ServletUtil.check_key(args,"Key");
    UKV.remove(key);
    return success("Removed key <strong>"+key.toString()+"</strong>");
  }

  @Override public String[] requiredArguments() {
    return new String[] { "Key" };
  }
}
