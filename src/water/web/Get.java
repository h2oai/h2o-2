package water.web;
import java.io.IOException;
import java.util.Properties;

import water.*;
import water.NanoHTTPD.Response;

public class Get extends Page {

  @Override public Object serve(Server server, Properties args, String sessionID) {
    try {
      String skey = args.getProperty("Key");
      Key key = Key.make(skey);
      if (!key.user_allowed()) throw new PageError("Not a user key: " + key);
      Value val = DKV.get(key);
      if( val == null ) {
        key = Key.make(skey);
        val = DKV.get(key);
      }
      if( val == null ) throw new PageError("Key not found: " + key);
      // HTML file save of Value
      Response res = server.new Response(NanoHTTPD.HTTP_OK,NanoHTTPD.MIME_DEFAULT_BINARY,val.openStream());
      res.addHeader("Content-Length", Long.toString(val.length()));
      res.addHeader("Content-Disposition", "attachment; filename="+key.toString());

      return res;
    } catch( IOException ex ) {
      return H2OPage.wrap(H2OPage.error(ex.toString()));
    } catch( PageError e ) {
      return H2OPage.wrap(H2OPage.error(e.getMessage()));
    }
  }

  @Override public String[] requiredArguments() {
    return new String[] { "Key" };
  }
}
