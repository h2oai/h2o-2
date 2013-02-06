package water.web;

import java.util.Properties;
import java.util.UUID;

import water.*;

import com.google.gson.JsonObject;

public class PutValue extends H2OPage {
  @Override public String[] requiredArguments() {
    return new String[] { "Value" };
  }

  @Override
  public JsonObject serverJson(Server s, Properties p, String sessionID) throws PageError {
    String keyS = p.getProperty("Key",UUID.randomUUID().toString());
    if( keyS.isEmpty() ) keyS = UUID.randomUUID().toString();

    int rf = getAsNumber(p, "RF", Key.DEFAULT_DESIRED_REPLICA_FACTOR);
    if( rf<0 || rf>127 ) throw new PageError("Replication factor must be from 0 to 127.");

    Key key;
    try {
      key = Key.make(keyS, (byte)rf);
    } catch( IllegalArgumentException e ) {
      throw new PageError("Not valid key: " + keyS);
    }

    String valS = p.getProperty("Value");
    Value val = new Value(key, valS);
    DKV.put(key, val);

    JsonObject res = new JsonObject();
    res.addProperty("key", key.toString());
    res.addProperty( "rf", rf);
    res.addProperty( "vsize", valS.length());
    return res;
  }

  @Override protected String serveImpl(Server s, Properties p, String sessionID) throws PageError {
    JsonObject json = serverJson(s, p, sessionID);
    RString response = new RString("" +
        "<div class='alert alert-success'>" +
        "Key <a href='Inspect?Key=%$key'>%key</a> has been put to the " +
        "store with replication factor %rf, value size <strong>%vsize</strong>." +
        "</div>" +
        "<p><a href='StoreView'><button class='btn btn-primary'>Back to Node</button></a>&nbsp;&nbsp;" +
        "<a href='Put'><button class='btn'>Put again</button></a>" +
        "</p>");
    response.replace(json);
    return response.toString();
  }
}
