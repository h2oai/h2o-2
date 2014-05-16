package water.api.rest;

import java.util.Map;

import water.api.CloudStatus;
import water.api.rest.Version.V1;
import water.api.rest.schemas.CloudStatusSchemaV1;

public class CloudStatusAdaptorV1 extends DeclarativeApiAdaptor<CloudStatus, CloudStatusSchemaV1, Version.V1> {

  @Override public V1 getVersion() { return Version.v1; }
  @Override public Class<CloudStatus> getImplClass() { return CloudStatus.class; }
  @Override public Class<CloudStatusSchemaV1> getApiClass() { return CloudStatusSchemaV1.class; }

  public static final String[][] pairs = map();
  static final Map<String, String> api2impl = toMap(pairs);
  static final Map<String, String> impl2api = toRMap(pairs);
  @Override protected Map<String, String> getAPI2Impl() {
    return api2impl;
  }
  @Override protected Map<String, String> getImpl2API() {
    return impl2api;
  }
}
