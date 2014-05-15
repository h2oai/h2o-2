package water.api.rest;

import water.Request2;
import water.api.rest.REST.ApiAdaptor;
import water.api.rest.REST.RestCall;

public abstract class ApiSupport<T extends ApiSupport, V extends Version> extends Request2 implements RestCall<V> {
  @Override protected Response serve() {
    // Get adaptor
    ApiAdaptor adaptor = REST.API_MAPPING.get(this.getClass());
    // Create implementation
    Request2 impl = (Request2) adaptor.makeImpl(this);
    Response r = impl.servePublic();
    // Fill API
    adaptor.fillApi(impl, this);
    return r;
  }
}