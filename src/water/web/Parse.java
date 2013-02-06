package water.web;
import java.util.Properties;
import java.util.UUID;

import water.*;
import water.parser.ParseDataset;

import com.google.gson.JsonObject;

public class Parse extends H2OPage {
  @Override public String[] requiredArguments() { return new String[] { "Key" }; }

  @Override
  public JsonObject serverJson(Server s, Properties p, String sessionID) throws PageError {
    String k = p.getProperty("Key");
    String rk = p.getProperty("Key2", UUID.randomUUID().toString());

    Key key = Key.make(k);
    Key resKey = Key.make(rk);

    JsonObject res = new JsonObject();
    res.addProperty("Key", resKey.toString());

    if( DKV.get(resKey) == null ) { // Key not parsed? Parse it
      long start = System.currentTimeMillis();
      Value dataset = DKV.get(key); // Get the source dataset root key
      if( dataset == null ) throw new PageError(key.toString()+" not found");
      try {
        ParseDataset.parse(resKey, dataset);
      } catch(IllegalArgumentException e) {
        throw new PageError(e.getMessage());
      } catch(Error e) {
        throw new PageError(e.getMessage());
      }

      long now = System.currentTimeMillis();
      res.addProperty("TimeMS", now - start);
    } else {
      res.addProperty("TimeMS", 0);
    }
    return res;

  }

  @Override protected String serveImpl(Server s, Properties p, String sessionID) throws PageError {
    JsonObject json = serverJson(s, p,sessionID);
    long time = json.get("TimeMS").getAsLong();

    RString res = time > 0
        ? new RString("Parsed into <a href='/Inspect?Key=%$Key'>%Key</a> in %TimeMS")
        : new RString("Already parsed into <a href='/Inspect?Key=%$Key'>%Key</a>.");
    res.replace("Key", json.get("Key").getAsString());
    res.replace("TimeMS", PrettyPrint.msecs(time, true));
    return res.toString();
  }
}
