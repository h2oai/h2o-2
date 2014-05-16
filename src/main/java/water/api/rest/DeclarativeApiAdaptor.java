package water.api.rest;

import java.util.*;

import water.Iced;
import water.api.rest.REST.AbstractApiAdaptor;
import water.api.rest.schemas.ApiSchema;
import water.util.Log;

public abstract class DeclarativeApiAdaptor<I extends Iced, A extends ApiSchema<? super V>, V extends Version> extends AbstractApiAdaptor<I, A, V> {
  static final String[] p(String a, String b) { return RestUtils.p(a, b); }
  static final String[][] map(String[] ...s)  { return RestUtils.map(s); }
  static final Map<String,String> toMap(String[][] pairs) {
    Map<String,String> r = new HashMap<String, String>(pairs.length);
    for (String[] p : pairs) r.put(p[0], p[1]);
    return Collections.unmodifiableMap(r);
  }
  static final Map<String,String> toRMap(String[][] pairs) {
    Map<String,String> r = new HashMap<String, String>(pairs.length);
    for (String[] p : pairs) r.put(p[1], p[0]);
    return Collections.unmodifiableMap(r);
  }

  abstract protected Map<String,String> getAPI2Impl();
  abstract protected Map<String,String> getImpl2API();

  @Override final public A fillApi(I impl, A api) {
    RestUtils.fillAPIFields(impl, api, REST.VAL_TRANSF);
    for (Map.Entry<String, String> e : getImpl2API().entrySet()) {
      boolean res = RestUtils.fillAPIField(impl, e.getValue(), api, e.getKey(), REST.VAL_TRANSF);
      if (!res) Log.warn("The field was not filled: " + e.getValue()+" --> " + e.getKey());
    }
    return fillA(impl, api);
  }
  @Override final public I fillImpl(A api, I impl) {
    RestUtils.fillImplFields(api, impl, REST.VAL_TRANSF);
    for (Map.Entry<String, String> e : getAPI2Impl().entrySet()) {
      boolean res = RestUtils.fillImplField(api, e.getValue(), impl, e.getKey(), REST.VAL_TRANSF);
      if (!res) Log.warn("The field was not filled: " + e.getValue()+" --> " + e.getKey());
    }
    return fillI(api,impl);
  }

  protected A fillA(I impl, A api) { return api;  }
  protected I fillI(A api, I impl) { return impl; }
}
