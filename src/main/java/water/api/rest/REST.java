package water.api.rest;

import java.util.HashMap;
import java.util.Map;

import water.*;

public class REST {

  public static Map<Class, ApiAdaptor> API_MAPPING = new HashMap<Class, ApiAdaptor>();
  static {
    API_MAPPING.put(GBMSchemaV1.class, new GBMAdaptorV1());
    API_MAPPING.put(GBMSchemaBloody.class, new GBMAdaptorBloody());
  }

  public static abstract class ApiSupport extends Request2 {
  }

  /** Abstract representation of a REST API call */
  interface RestCall<T extends Version> {
    public T getVersion();
  }

  interface ApiAdaptor<I extends Iced, O extends RestCall<? super V>, V extends Version> {
      public abstract I makeImpl(O api);
      public abstract O makeApi(I impl);
      // Transfer inputs from API to implementation
      public abstract I fillImpl(O api, I impl);
      // Transfer outputs from implementation to API
      public abstract O fillApi (I impl, O api);
      public abstract V getVersion();
      // Just creates implementation object
      public abstract I createImpl();
      // Just creates api object
      public abstract O createApi();
  }

  public static abstract class AbstractApiAdaptor<I extends Iced, O extends RestCall<? super V>, V extends Version> implements ApiAdaptor<I,O,V> {
    @Override public I makeImpl(O api) {
      I impl = createImpl();
      return fillImpl(api, impl);
    }
    @Override public O makeApi(I impl) {
      O api = createApi();
      return fillApi(impl, api);
    }
  }
}

