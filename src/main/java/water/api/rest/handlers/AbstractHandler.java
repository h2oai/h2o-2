package water.api.rest.handlers;

import water.*;
import water.api.rest.*;
import water.api.rest.REST.Versioned;
import water.api.rest.schemas.ApiSchema;

import java.util.Properties;

public abstract class AbstractHandler<V extends Version> implements Versioned<V> {

  private String path = null;

  protected AbstractHandler() { }

  public AbstractHandler(String path) {
    this.path = path;
  }



  public Iced findObject(String path) {
    // TODO: error checking!
    String key = path.substring(path.lastIndexOf("/") + 1);
    return DKV.get(Key.make(key)).get();
  }

  public NanoHTTPD.Response get(NanoHTTPD server, String path, Properties header, Properties parms) {
    // find the object and return it
    Iced impl = findObject(path); // TODO: null check!
    ApiAdaptor adaptor = REST.getAdaptorFromImpl(impl.getClass());
    ApiSchema api = adaptor.createAndAdaptToApi(impl);
    String value = new String(api.writeJSON(new AutoBuffer()).buf());
    return server.new Response(NanoHTTPD.HTTP_OK, NanoHTTPD.MIME_JSON, value);
  }

  public NanoHTTPD.Response post(NanoHTTPD server, String path, Properties header, Properties parms) {
/*
// Get adaptor
ApiAdaptor adaptor = REST.getAdaptor(this.getClass());
// Create implementation
Request2 impl = (Request2) adaptor.createAndAdaptToImpl(adaptor.createApi());
RequestBuilders.Response r = impl.servePublic();
// Fill API
adaptor.fillApi(impl, this);
String value = new String(r._req.writeJSON(new AutoBuffer()).buf());
return server.new Response(NanoHTTPD.HTTP_OK, NanoHTTPD.MIME_JSON, value);
*/
    throw new UnsupportedOperationException("Don't know how to PUT at path: " + path);

  }

  public NanoHTTPD.Response put(NanoHTTPD server, String path, Properties header, Properties parms) {
    throw new UnsupportedOperationException("Don't know how to PUT at path: " + path);
  }

  public NanoHTTPD.Response delete(NanoHTTPD server, String path, Properties header, Properties parms) {
    throw new UnsupportedOperationException("Don't know how to DELETE at path: " + path);
  }
}
