package water.api.rest;

import hex.gbm.GBM;
import hex.gbm.GBM.GBMModel;

import java.util.Map;

import water.api.rest.Version.V1;
import water.api.rest.schemas.GBMSchemaV1;

/**
 * Adaptor transforming GBM.GBMModel v1 to implementation.
 *
 */
public class GBMAdaptorV1 extends DeclarativeApiAdaptor<GBM.GBMModel, GBMSchemaV1, Version.V1> {

  /** List of attributes for API declaring mapping from API to implementation. */
  // TODO: do the pairs with matching names automagically in DeclarativeApiAdaptor
  /*
  public static final String[][] pairs = map(
      p("source", "source"), p("cols", "cols"), p("response", "response"),
      p("ntrees", "ntrees"), p("learn_rate", "learn_rate") );
  */
  public static final String[][] pairs = map(
      p("learn_rate", "learn_rate") );


  /// ---- All these methods and fields can be automatically generated
  // Get the class for the implementation object
  @Override public Class getImplClass() { return GBM.GBMModel.class; }
  // Get the class for the api object
  @Override public Class getApiClass() { return GBMSchemaV1.class; }

  /** Helper map from API attributes to implementation attributes. */
  static final Map<String, String> api2impl = toMap(pairs);
  /** Helper map from implementation attributes to API attributes. */
  static final Map<String, String> impl2api = toRMap(pairs);

  @Override public V1 getVersion() {  return Version.v1; }
  @Override protected Map<String, String> getAPI2Impl() {
    return api2impl;
  }
  @Override protected Map<String, String> getImpl2API() {
    return impl2api;
  }


  /// ------------- end of automatic generation ---------

  @Override protected GBM.GBMModel fillI(GBMSchemaV1 api, GBM.GBMModel impl) {
    // impl.response = impl.source != null ? impl.source.vec(api.response) : null;
    return impl;
  }
}
