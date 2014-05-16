package water.api.rest.handlers;

import water.*;
import water.api.RequestServer;
import water.api.rest.*;
import water.api.rest.REST.Versioned;
import water.api.rest.schemas.ApiSchema;

import java.util.Properties;

public abstract class DKVHandler<V extends Version> extends AbstractHandler<V> {

  protected DKVHandler() { }

  public DKVHandler(String path) {
    super(path);
  }

  public Iced findObject(String path) {
    if (-1 == path.lastIndexOf("/"))
      return null;

    String key = path.substring(path.lastIndexOf("/") + 1);
    Value v = DKV.get(Key.make(key));
    if (null == v)
        return null;

    return v.get();
  }

  public NanoHTTPD.Response get(NanoHTTPD server, String path, Properties header, Properties parms) {
    // find the object and return it
    Iced impl = findObject(path);

    if (null == impl)
      return RequestServer.response404(server, parms);

    ApiAdaptor adaptor = REST.getAdaptorFromImpl(impl.getClass());
    ApiSchema api = adaptor.createAndAdaptToApi(impl);
    String value = new String(api.writeJSON(new AutoBuffer()).buf());
    return server.new Response(NanoHTTPD.HTTP_OK, NanoHTTPD.MIME_JSON, value);
  }

  public NanoHTTPD.Response post(NanoHTTPD server, String path, Properties header, Properties parms) {
    throw new UnsupportedOperationException("Don't know how to PUT at path: " + path);
  }

  public NanoHTTPD.Response put(NanoHTTPD server, String path, Properties header, Properties parms) {
    // mutate by overwriting
    return post(server, path, header, parms);
  }

  public NanoHTTPD.Response delete(NanoHTTPD server, String path, Properties header, Properties parms) {
    throw new UnsupportedOperationException("Don't know how to DELETE at path: " + path);
  }
}
