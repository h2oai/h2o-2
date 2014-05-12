package water.api.rest;

import hex.gbm.GBM;
import water.api.rest.REST.AbstractApiAdaptor;
import water.api.rest.Version.V1;

public class GBMAdaptorV1 extends AbstractApiAdaptor<GBM, GBMSchemaV1, Version.V1> {

  @Override public GBMSchemaV1 createApi() { return new GBMSchemaV1(); }
  @Override public GBM createImpl() { return new GBM();  }
  @Override public V1 getVersion() {  return Version.v1; }

  @Override public GBM fillImpl(GBMSchemaV1 api, GBM impl) {
    // Transfers API -> impl parameters
    impl.source = api.source;
    impl.cols = api.cols;
    impl.ntrees = api.ntrees;
    impl.learn_rate = api.learn_rate;
    impl.response = api.source.vec(api.response);
    return impl;
  }

  @Override public GBMSchemaV1 fillApi(GBM impl, GBMSchemaV1 api) {
    // Transfers impl output -> API
    api.ntrees = impl.ntrees;
    api.learn_rate = impl.learn_rate;
    api.destination_key = impl.destination_key;
    api.job_key = impl.job_key;
    return api;
  }

}