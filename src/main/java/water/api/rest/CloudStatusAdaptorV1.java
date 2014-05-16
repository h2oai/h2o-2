package water.api.rest;

import java.util.Map;

import water.api.CloudStatus;
import water.api.rest.Version.V1;
import water.api.rest.schemas.CloudStatusSchemaV1;

public class CloudStatusAdaptorV1 extends DeclarativeApiAdaptor<CloudStatus, CloudStatusSchemaV1, Version.V1> {

  // TODO: getVersion will be done in the weaver Real Soon Now (tm)
  @Override public V1 getVersion() { return Version.v1; }

  // TODO: getImplClass and getApiClass will be done in the weaver a little later
  @Override public Class<CloudStatus> getImplClass() { return CloudStatus.class; }
  @Override public Class<CloudStatusSchemaV1> getApiClass() { return CloudStatusSchemaV1.class; }
}
