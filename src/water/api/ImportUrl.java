package water.api;

import java.io.*;
import java.net.URL;

import water.Key;
import water.ValueArray;

import com.google.common.io.Closeables;
import com.google.gson.JsonObject;

public class ImportUrl extends Request {
  private final Str _url = new Str(URL);
  private final H2OKey _key = new H2OKey(KEY, (Key)null);

  public ImportUrl() {
    _requestHelp = "Imports the given URL.";
    _url._requestHelp = "URL to import.";
    _key._requestHelp = "Key to store the url contents under.";
  }

  @Override
  protected Response serve() {
    InputStream s = null;
    String urlStr = _url.value();
    try {
      if( urlStr.startsWith("file://") ) {
        urlStr = urlStr.substring("file://".length());
        File f = new File(urlStr);
        urlStr = "file://"+f.getCanonicalPath();
      }

      URL url = new URL(urlStr);
      Key k = _key.value();
      if( k == null ) k = Key.make(urlStr);
      s = url.openStream();
      if( s == null )
        return Response.error("Unable to open stream to URL "+url.toString());
      k = ValueArray.readPut(k, s);

      JsonObject json = new JsonObject();
      json.addProperty(KEY, k.toString());
      json.addProperty(URL, urlStr);
      Response r = Response.done(json);
      r.setBuilder(KEY, new KeyElementBuilder());
      return r;
    } catch( IllegalArgumentException e ) {
      return Response.error("Not a valid key: "+ urlStr);
    } catch( IOException e ) {
      return Response.error(e.getMessage());
    } finally {
      Closeables.closeQuietly(s);
    }
  }
}
