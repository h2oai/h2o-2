package water.web;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Properties;

import com.google.common.io.Closeables;
import com.google.gson.JsonObject;
import java.io.File;

import water.Key;
import water.ValueArray;

public class ImportUrl extends H2OPage {

  public static Key importUrl(String keyName, String urlStr) throws Exception {
    if (urlStr.startsWith("file://")) {
      urlStr = urlStr.substring("file://".length());
      File f = new File(urlStr);
      urlStr = "file://"+f.getCanonicalPath();
    }
    URL url;
    try {
      url = new URL(urlStr);
    } catch( MalformedURLException ex ) {
      throw new Exception("Malformed url: "+urlStr);
    }
    if ((keyName == null) || keyName.isEmpty())
      keyName = urlStr;

    try {
       Key.make(keyName);
    } catch( IllegalArgumentException e ) {
      throw new Exception("Not a valid key: "+ keyName);
    }
    InputStream s = null;
    try {
      s = url.openStream();
      if( s==null ) throw new Exception("Unable to open stream to URL "+url.toString());
      Key result = ValueArray.readPut(keyName, s);
      return result;
    } catch (IOException e) {
      throw new Exception(e.getMessage());
    } finally {
      Closeables.closeQuietly(s);
    }
  }


  protected static Key importUrl(Properties args) throws Exception {
    String keyName = args.getProperty("Key",null);
    String url = args.getProperty("Url");
    return importUrl(keyName, url);
  }

  @Override public JsonObject serverJson(Server server, Properties args, String sessionID) throws PageError {
    JsonObject result = new JsonObject();
    try {
      Key k = importUrl(args);
      result.addProperty("Key",k.toString());
    } catch (Exception e) {
      result.addProperty("Error",e.getMessage());
    }
    return result;
  }

  @Override protected String serveImpl(Server server, Properties args, String sessionID) {
    try {
      Key k = importUrl(args);
      return success("Url "+args.getProperty("Url")+" imported as key <strong>"+k.toString()+"</strong>");
    } catch (Exception e) {
      return error("Unable to import url "+args.getProperty("Url")+" due to the following error:<br />"+e.toString());
    }
  }

  @Override public String[] requiredArguments() {
    return new String[] { "Url" };
  }
}
