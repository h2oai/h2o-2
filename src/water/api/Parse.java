
package water.api;

import water.Key;
import water.Value;
import water.parser.ParseDataset;
import water.web.RString;

import com.google.gson.JsonObject;

public class Parse extends Request {
  protected final H2OExistingKey _source = new H2OExistingKey(SOURCE_KEY);
  protected final H2OKey _dest = new H2OKey(DEST_KEY, (Key)null);

  public static String link(Key k, String content) {
    RString rs = new RString("<a href='Parse.html?%key_param=%$key'>%content</a>");
    rs.replace("key_param", SOURCE_KEY);
    rs.replace("key", k.toString());
    rs.replace("content", content);
    return rs.toString();
  }

  @Override protected Response serve() {
    Value source = _source.value();
    Key dest = _dest.value();
    if (dest == null) {
      String n = source._key.toString();
      int dot = n.lastIndexOf('.');
      if( dot > 0 ) n = n.substring(0, dot);
      dest = Key.make(n+".hex");
    }
    try {
      ParseDataset.forkParseDataset(dest, source);
      JsonObject response = new JsonObject();
      response.addProperty(RequestStatics.DEST_KEY,dest.toString());

      Response r = ParseProgress.redirect(response, dest);
      r.setBuilder(RequestStatics.DEST_KEY, new KeyElementBuilder());
      return r;
    } catch (IllegalArgumentException e) {
      return Response.error(e.getMessage());
    } catch (Error e) {
      return Response.error(e.getMessage());
    }
  }

}
