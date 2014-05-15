package water.api.rest;

import water.api.rest.schemas.GBMSchemaBloody;
import hex.gbm.GBM;

public class GBMAdaptorBloody extends BloodyApiAdaptor<GBM, GBMSchemaBloody> {

  @Override public GBMSchemaBloody createApi() { return new GBMSchemaBloody(); }
  @Override public GBM createImpl() { return new GBM();  }
}