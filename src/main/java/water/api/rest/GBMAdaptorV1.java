package water.api.rest;

import hex.gbm.GBM;

import java.util.Map;

import water.api.rest.Version.V1;
import water.api.rest.schemas.GBMSchemaV1;

/**
 * Adaptor transforming GBM v1 to implementation.
 *
 */
public class GBMAdaptorV1 extends DeclarativeApiAdaptor<GBM, GBMSchemaV1, Version.V1> {

  /** List of attributes for API declaring mapping from API to implementation. */
  public static final String[][] pairs = map(
      p("source", "source"), p("cols", "cols"), p("response", "response"),
      p("ntrees", "ntrees"), p("learn_rate", "learn_rate") );


  /// ---- All these methods and fields can be automatically generated
  /** Helper map from API attributes to implementation attributes. */
  static final Map<String, String> api2impl = toMap(pairs);
  /** Helper map from implementation attributes to API attributes. */
  static final Map<String, String> impl2api = toRMap(pairs);

  @Override public GBMSchemaV1 createApi() { return new GBMSchemaV1(); }
  @Override public GBM createImpl() { return new GBM();  }
  @Override public V1 getVersion() {  return Version.v1; }
  @Override protected Map<String, String> getAPI2Impl() {
    return api2impl;
  }
  @Override protected Map<String, String> getImpl2API() {
    return impl2api;
  }
  /// ------------- end of automatic generation ---------

  @Override protected GBM fillI(GBMSchemaV1 api, GBM impl) {
    impl.response = impl.source != null ? impl.source.vec(api.response) : null;
    return impl;
  }
}