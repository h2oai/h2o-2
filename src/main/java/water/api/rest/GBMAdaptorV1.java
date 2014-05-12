package water.api.rest;

import hex.gbm.GBM;
import water.api.rest.REST.ApiAdaptor;
import water.api.rest.Version.V1;

public class GBMAdaptorV1 implements ApiAdaptor<GBM, GBMAPI, Version.V1> {

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
    impl.response = api.source.vec(api.response);
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