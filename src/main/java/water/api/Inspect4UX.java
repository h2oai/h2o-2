package water.api;

import java.io.InputStream;

import water.*;
import water.util.Log;
import water.util.RString;

import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;


public class Inspect4UX extends Inspect {
  static private String _inspectTemplate;

  static {
    InputStream resource = Boot._init.getResource2("/Inspect.html");
    try {
      _inspectTemplate = new String(ByteStreams.toByteArray(resource)).replace("%cloud_name", H2O.NAME);
    } catch( NullPointerException e ) {
      if( !Log._dontDie ) {
        Log.err(e);
        Log.die("Inspect.html not found in resources.");
      }
    } catch( Exception e ) {
      Log.err(e);
      Log.die(e.getMessage());
    } finally {
      Closeables.closeQuietly(resource);
    }
  }

  @Override protected String htmlTemplate() {
    return _inspectTemplate;
  }

  public static String link(Key k, String content) {
    RString rs = new RString("<a href='/Inspect4UX.html?%key_param=%$key'>%content</a>");
    rs.replace("key_param", KEY);
    rs.replace("key", k.toString());
    rs.replace("content", content);
    return rs.toString();
  }
}