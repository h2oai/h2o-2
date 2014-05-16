package water.api.rest;

import hex.gbm.GBM;
import water.api.rest.schemas.GBMSchemaV1;

/**
 * Adaptor transforming GBM.GBMModel v1 to implementation.
 *
 */
public class GBMAdaptorV1 extends DeclarativeApiAdaptor<GBM.GBMModel, GBMSchemaV1, Version.V1> {

  // TODO: getImplClass and getApiClass will be done in the weaver a little later
  @Override public Class getImplClass() { return GBM.GBMModel.class; }
  // TODO: getImplClass and getApiClass will be done in the weaver a little later
  @Override public Class getApiClass() { return GBMSchemaV1.class; }

  @Override protected GBM.GBMModel fillI(GBMSchemaV1 api, GBM.GBMModel impl) {
    // impl.response = impl.source != null ? impl.source.vec(api.response) : null;
    return impl;
  }
}
