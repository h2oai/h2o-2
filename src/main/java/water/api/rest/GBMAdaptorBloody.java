package water.api.rest;

import water.api.rest.schemas.GBMSchemaBloody;
import hex.gbm.GBM;

public class GBMAdaptorBloody extends BloodyApiAdaptor<GBM, GBMSchemaBloody> {
    // Get the class for the implementation object
    @Override public Class getImplClass() { return GBM.class; }
    // Get the class for the api object
    @Override public Class getApiClass() { return GBMSchemaBloody.class; }
}
