package water.api.rest;

import water.Iced;
import water.api.rest.schemas.ApiSchema;
import water.util.Log;

public abstract class AbstractApiAdaptor<I extends Iced, A extends ApiSchema<? super V>, V extends Version> implements ApiAdaptor<I,A,V> {
  @Override public A createApi() {
    A api = null;
    try {
      api = getApiClass().newInstance();
    }
    catch (Exception e) {
      Log.warn("Caught exception trying to create a: " + getApiClass().toString());
    }
    return api;
  }

  @Override public I createImpl() {
    I impl = null;
    try {
      impl = getImplClass().newInstance();
    }
    catch (Exception e) {
      Log.warn("Caught exception trying to create a: " + getImplClass().toString());
    }
    return impl;
  }

  @Override public I createAndAdaptToImpl(A api) {
    I impl = createImpl();
    return fillImpl(api, impl);
  }
  @Override public A createAndAdaptToApi(I impl) {
    A api = createApi();
    return fillApi(impl, api);
  }

  @Override public V getVersion() {
    throw RestUtils.barf(this);
  }
}