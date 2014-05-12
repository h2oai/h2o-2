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

  /**
   * @param <I> implementation type
   * @param <A> api type
   * @param <V> version type
   */
  interface ApiAdaptor<I extends Iced, A extends RestCall<? super V>, V extends Version> {
      // Make implementation object based on given api object
      public abstract I makeImpl(A api);
      // Make API object based on implementation object
      public abstract A makeApi(I impl);
      // Transfer inputs from API to implementation
      public abstract I fillImpl(A api, I impl);
      // Transfer outputs from implementation to API
      public abstract A fillApi (I impl, A api);
      // Get supported version
      public abstract V getVersion();
      // Just creates empty implementation object
      public abstract I createImpl();
      // Just creates empty API object
      public abstract A createApi();
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

