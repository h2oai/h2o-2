package water.api.rest;

import java.util.HashMap;
import java.util.Map;

import water.*;

public class REST {

  public static Map<Class, ApiAdaptor> API_MAPPING = new HashMap<Class, ApiAdaptor>();
  static {
    API_MAPPING.put(GBMAPI.class, new GBMAdaptorV1());
  }

  public static abstract class ApiSupport extends Request2 {
  }

  /** Abstract representation of a REST API call */
  interface RestCall<T extends Version> {
    public T getVersion();
  }

  interface ApiAdaptor<I extends Iced, O extends RestCall<V>, V extends Version> {
      public abstract I makeImpl(O api);
      public abstract O makeApi(I impl);
      // Transfer inputs from API to implementation
      public abstract I fillImpl(O api, I impl);
      // Transfer outputs from implementation to API
      public abstract O fillApi (I impl, O api);
      public abstract V getVersion();
  }
}

