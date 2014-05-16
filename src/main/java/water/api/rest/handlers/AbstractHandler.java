package water.api.rest.handlers;

import java.util.Properties;

import water.NanoHTTPD;
import water.api.rest.REST.Versioned;
import water.api.rest.*;

public abstract class AbstractHandler<V extends Version> implements Versioned<V> {

  private String path = null;

  protected AbstractHandler() { }

  public AbstractHandler(String path) {
    this.path = path;
  }

  public NanoHTTPD.Response get(NanoHTTPD server, String path, Properties header, Properties parms) {
    throw new UnsupportedOperationException("Don't know how to PUT at path: " + path);
  }

  public NanoHTTPD.Response post(NanoHTTPD server, String path, Properties header, Properties parms) {
    throw new UnsupportedOperationException("Don't know how to PUT at path: " + path);
  }

  public NanoHTTPD.Response put(NanoHTTPD server, String path, Properties header, Properties parms) {
    throw new UnsupportedOperationException("Don't know how to PUT at path: " + path);
  }

  public NanoHTTPD.Response delete(NanoHTTPD server, String path, Properties header, Properties parms) {
    throw new UnsupportedOperationException("Don't know how to DELETE at path: " + path);
  }

  @Override public V getVersion() {
    throw RestUtils.barf(this);
  }
}
