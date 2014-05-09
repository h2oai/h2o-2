package water.api.rest;

import java.util.HashMap;
import java.util.Map;

import hex.gbm.GBM;
import water.*;
import water.api.rest.Version.V1;

public class REST {

  public static Map<Class, ApiAdaptor> API_MAPPING = new HashMap<Class, ApiAdaptor>();
  static {
    API_MAPPING.put(GBMAPI.class, new GBMAdaptor());
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

  public static class GBMAdaptor implements ApiAdaptor<GBM, GBMAPI, Version.V1> {

    @Override
    public GBM makeImpl(GBMAPI o) {
      GBM gbm = new GBM();
      return fillImpl(o, gbm);
    }

    @Override
    public GBMAPI makeApi(GBM impl) {
      GBMAPI api = new GBMAPI();
      return fillApi(impl, api);
    }

    @Override public V1 getVersion() {
      return Version.v1;
    }

    @Override public GBM fillImpl(GBMAPI api, GBM impl) {
      impl.source = api.source;
      impl.cols = api.cols;
      impl.ntrees = api.ntrees;
      impl.learn_rate = api.learn_rate;
      return impl;
    }

    @Override public GBMAPI fillApi(GBM impl, GBMAPI api) {
      api.ntrees = impl.ntrees;
      api.learn_rate = impl.learn_rate;
      api.destination_key = impl.destination_key;
      api.job_key = impl.job_key;
      return api;
    }
  }
}

