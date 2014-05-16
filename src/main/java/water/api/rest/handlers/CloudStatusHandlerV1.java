package water.api.rest.handlers;

import water.api.CloudStatus;
import water.api.rest.Version;

public class CloudStatusHandlerV1 extends DKVHandler<Version.V1> {
  public CloudStatusHandlerV1(String path) {
    super(path);
  }

  // TODO: getVersion will be done in the weaver Real Soon Now (tm)
  @Override public Version.V1 getVersion() {
    return Version.v1;
  }

  @Override public water.Iced findObject(String path) {
    CloudStatus cs = new CloudStatus();
    cs.cloud_name = "TEST";
    return cs;
  };

}
